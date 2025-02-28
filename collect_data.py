import os
import json
import duckdb
import time
from requests import Session
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

load_dotenv()

TBA_PREFIX = "https://www.thebluealliance.com/api/v3/"
TBA_AUTH_KEY = os.getenv('TBA_AUTH_KEY')
ENV = os.getenv('ENV')

def get_tba(url: str):
    file_path = 'cache/tba/' + url.replace('/', '-') + ".json"

    folder_path = os.path.dirname('cache/tba/' + url.replace('/', '-') + '.json')

    if ENV == "DEV" and not os.path.exists(folder_path):
        os.makedirs(folder_path)

    if ENV == "DEV" and os.path.isfile(file_path):
        with open(file_path, 'r') as file:
            return json.load(file)
    else:
        session = Session()
        session.headers.update({"X-TBA-Auth-Key": TBA_AUTH_KEY, "X-TBA-Auth-Id": ""})
        print(TBA_PREFIX + url)
        response = session.get(TBA_PREFIX + url)
        if response.status_code == 200:
            if ENV == "DEV":
                with open(file_path, 'w') as file:
                    file.write(json.dumps(response.json()))
            return response.json()
        else:
            raise Exception(f"TBA Call Failed Error: {url}")


def save_events(district_key: str, con: duckdb.DuckDBPyConnection):
    events = get_tba(f'district/{district_key}/events')
    event_keys = []
    con.execute("CREATE TABLE IF NOT EXISTS events (district_key VARCHAR, event_key VARCHAR, name VARCHAR, event_type VARCHAR, start_date DATE, end_date DATE)")
    for event in events:
        con.execute("INSERT INTO events (district_key, event_key, name, event_type, start_date, end_date) VALUES (?, ?, ?, ?, ?, ?)", (district_key, event["key"], event["short_name"], event["event_type_string"], event["start_date"], event["end_date"]))
        event_keys.append((event["key"], event["start_date"]))
    return event_keys

def save_event_teams(event_key: str, con: duckdb.DuckDBPyConnection):
    teams = get_tba(f'event/{event_key}/teams')
    con.execute("CREATE TABLE IF NOT EXISTS event_teams (event_key VARCHAR, team_key VARCHAR)")
    for team in teams:
        con.execute("INSERT INTO event_teams (event_key, team_key) VALUES (?, ?)", (event_key, team["key"]))

def save_matches(event_key: str, con: duckdb.DuckDBPyConnection, fetch_data: bool):
    con.execute("CREATE TABLE IF NOT EXISTS matches (match_key VARCHAR, event_key VARCHAR, comp_level VARCHAR, match_number INT, winning_alliance VARCHAR, red_1_key VARCHAR, red_2_key VARCHAR, red_3_key VARCHAR, blue_1_key VARCHAR, blue_2_key VARCHAR, blue_3_key VARCHAR)")
    if fetch_data:
        matches = get_tba(f'event/{event_key}/matches/simple')
        for match in matches:
            con.execute(
                "INSERT INTO matches (match_key, event_key, comp_level, match_number, winning_alliance, red_1_key, red_2_key, red_3_key, blue_1_key, blue_2_key, blue_3_key) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    match["key"],
                    event_key,
                    match["comp_level"],
                    match["match_number"],
                    match["winning_alliance"],
                    match["alliances"]["red"]["team_keys"][0],
                    match["alliances"]["red"]["team_keys"][1],
                    match["alliances"]["red"]["team_keys"][2],
                    match["alliances"]["blue"]["team_keys"][0],
                    match["alliances"]["blue"]["team_keys"][1],
                    match["alliances"]["blue"]["team_keys"][2]
                )
            )

def save_alliances(event_key: str, con: duckdb.DuckDBPyConnection, fetch_data: bool):
    con.execute("CREATE TABLE IF NOT EXISTS alliances (event_key VARCHAR, alliance_name VARCHAR, captain_key VARCHAR, first_selection_key VARCHAR, second_selection_key VARCHAR, backup_key VARCHAR)")
    if fetch_data:
        alliances = get_tba(f'event/{event_key}/alliances') or []
        for alliance in alliances:
            con.execute(
                "INSERT INTO alliances (event_key, alliance_name, captain_key, first_selection_key, second_selection_key, backup_key) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    event_key,
                    alliance["name"],
                    alliance["picks"][0],
                    alliance["picks"][1],
                    alliance["picks"][2],
                    alliance["backup"]["in"] if "backup" in alliance else None
                )
            )

def save_awards(event_key: str, con: duckdb.DuckDBPyConnection, fetch_data: bool):
    con.execute("CREATE TABLE IF NOT EXISTS awards (event_key VARCHAR, award_type VARCHAR, team_key VARCHAR)")
    if fetch_data:
        awards = get_tba(f'event/{event_key}/awards')
        for award in awards:
            for recipient in award["recipient_list"]:
                con.execute(
                    "INSERT INTO awards (event_key, award_type, team_key) VALUES (?, ?, ?)",
                    (award["event_key"], award["award_type"], recipient["team_key"])
                )

def save_event_points(event_key: str, con: duckdb.DuckDBPyConnection, fetch_data: bool):
    con.execute("CREATE TABLE IF NOT EXISTS event_points (event_key VARCHAR, team_key VARCHAR, points INT)")
    if fetch_data:
        data = get_tba(f'event/{event_key}/district_points')
        event_points = data["points"] if data else {}
        for team in event_points.keys():
            con.execute("INSERT INTO event_points (event_key, team_key, points) VALUES (?, ?, ?)", (event_key, team, event_points[team]["total"]))

def save_district_rankings(district_key: str, con: duckdb.DuckDBPyConnection):
    rankings = get_tba(f'district/{district_key}/rankings')
    con.execute("CREATE TABLE IF NOT EXISTS district_rankings (district_key VARCHAR, team_key VARCHAR, rank INT, points INT, rookie_bonus INT)")
    for ranking in rankings:
        con.execute("INSERT INTO district_rankings (district_key, team_key, rank, points, rookie_bonus) VALUES (?, ?, ?, ?, ?)", (district_key, ranking["team_key"], ranking["rank"], ranking["point_total"], ranking["rookie_bonus"]))


def collect_event_data(event_key: str, start_date: str, con: duckdb.DuckDBPyConnection):
    fetch_data = datetime.strptime(start_date, '%Y-%m-%d') <= datetime.now()
    save_event_teams(event_key, con)
    save_matches(event_key, con, fetch_data)
    save_alliances(event_key, con, fetch_data)
    save_awards(event_key, con, fetch_data)
    save_event_points(event_key, con, fetch_data)

def collect_data(district_key: str, con: duckdb.DuckDBPyConnection):
    event_keys = save_events(district_key, con)
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(collect_event_data, event_key, start_date, con) for event_key, start_date in event_keys]
        for future in as_completed(futures):
            future.result()
    save_district_rankings(district_key, con)


def main():
    con = duckdb.connect()
    collect_data('2024fma', con)
    con.close()

if __name__ == "__main__":
    main()