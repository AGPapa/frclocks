import os
import json
import base64
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

FIRST_API_BASE = "https://frc-api.firstinspires.org/v3.0/"


def _first_api_auth_header():
    raw = os.getenv("FIRST_API_AUTH", "").strip()
    if not raw:
        user = os.getenv("FIRST_API_USERNAME", "").strip()
        tok = os.getenv("FIRST_AUTH_TOKEN", "").strip()
        if user and tok:
            raw = f"{user}:{tok}"
        elif tok and ":" in tok:
            raw = tok
    if not raw:
        return None
    if os.getenv("FIRST_API_BASIC_B64", "").strip():
        encoded = os.getenv("FIRST_API_BASIC_B64", "").strip()
    else:
        encoded = base64.b64encode(raw.encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {encoded}", "Accept": "application/json"}


def get_first(path: str, params: dict | None = None):
    auth = _first_api_auth_header()
    if not auth:
        raise Exception("FIRST API auth not configured (see FIRST_API_AUTH or FIRST_API_USERNAME + FIRST_AUTH_TOKEN)")

    safe = path.replace("/", "-")
    param_key = ""
    if params:
        param_key = "-" + "-".join(f"{k}-{v}" for k, v in sorted(params.items()))
    file_path = f"cache/first/{safe}{param_key}.json"
    folder_path = os.path.dirname(file_path)
    if ENV == "DEV" and not os.path.exists(folder_path):
        os.makedirs(folder_path)

    if ENV == "DEV" and os.path.isfile(file_path):
        with open(file_path, "r") as file:
            return json.load(file)

    session = Session()
    session.headers.update(auth)
    url = FIRST_API_BASE + path.lstrip("/")
    response = session.get(url, params=params or {})
    if response.status_code == 200:
        data = response.json()
        if ENV == "DEV":
            with open(file_path, "w") as file:
                file.write(json.dumps(data))
        return data
    raise Exception(f"FIRST API failed {response.status_code}: {url} {params!r}")


def _tba_district_key_to_first_code(district_key: str) -> tuple[int, str]:
    year = int(district_key[:4])
    code = district_key[4:].upper()
    return year, code


def get_first_district_rankings_pages(district_key: str) -> list[dict]:
    year, code = _tba_district_key_to_first_code(district_key)
    combined = []
    page = 1
    page_total = 1
    while page <= page_total:
        data = get_first(f"{year}/rankings/district", params={"districtCode": code, "page": page})
        rows = data.get("districtRanks") or []
        for row in rows:
            team_num = row["teamNumber"]
            combined.append(
                {
                    "team_key": f"frc{team_num}",
                    "rank": int(row["rank"]),
                    "point_total": int(row["totalPoints"]),
                    "rookie_bonus": int(row.get("teamAgePoints") or 0),
                }
            )
        page_total = int(data.get("pageTotal") or 1)
        page += 1
    return combined

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
        if event["key"] in ("2025tempclone-356125237", "2026txabi"):
            continue
        con.execute("INSERT INTO events (district_key, event_key, name, event_type, start_date, end_date) VALUES (?, ?, ?, ?, ?, ?)", (district_key, event["key"], event["short_name"], event["event_type_string"], event["start_date"], event["end_date"]))
        event_keys.append((event["key"], event["start_date"]))
    return event_keys

def save_event_teams(event_key: str, con: duckdb.DuckDBPyConnection):
    teams = get_tba(f'event/{event_key}/teams')
    con.execute("CREATE TABLE IF NOT EXISTS event_teams (event_key VARCHAR, team_key VARCHAR)")
    for team in teams:
        con.execute("INSERT INTO event_teams (event_key, team_key) VALUES (?, ?)", (event_key, team["key"]))

def save_matches(event_key: str, con: duckdb.DuckDBPyConnection, fetch_data: bool):
    con.execute("CREATE TABLE IF NOT EXISTS matches (match_key VARCHAR, event_key VARCHAR, comp_level VARCHAR, match_number INT, winning_alliance VARCHAR, red_score INT, blue_score INT, red_1_key VARCHAR, red_2_key VARCHAR, red_3_key VARCHAR, blue_1_key VARCHAR, blue_2_key VARCHAR, blue_3_key VARCHAR)")
    if fetch_data:
        matches = get_tba(f'event/{event_key}/matches/simple')
        for match in matches:
            con.execute(
                "INSERT INTO matches (match_key, event_key, comp_level, match_number, winning_alliance, red_score, blue_score, red_1_key, red_2_key, red_3_key, blue_1_key, blue_2_key, blue_3_key) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    match["key"],
                    event_key,
                    match["comp_level"],
                    match["match_number"],
                    match["winning_alliance"],
                    match["alliances"]["red"]["score"],
                    match["alliances"]["blue"]["score"],
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
                    alliance["picks"][3] if len(alliance["picks"]) > 3 else None
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
    con.execute("CREATE TABLE IF NOT EXISTS event_points (event_key VARCHAR, team_key VARCHAR, points INT, qual_points INT, selection_points INT, elim_points INT, award_points INT)")
    if fetch_data:
        data = get_tba(f'event/{event_key}/district_points')
        event_points = data["points"] if data else {}
        for team in event_points.keys():
            con.execute("INSERT INTO event_points (event_key, team_key, points, qual_points, selection_points, elim_points, award_points) VALUES (?, ?, ?, ?, ?, ?, ?)", (event_key, team, event_points[team]["total"], event_points[team]["qual_points"], event_points[team]["alliance_points"], event_points[team]["elim_points"], event_points[team]["award_points"]))

def save_district_rankings(district_key: str, con: duckdb.DuckDBPyConnection):
    sources = ["first", "tba"]
    rankings = None
    last_err = None
    for src in sources:
        try:
            if src == "first":
                rankings = get_first_district_rankings_pages(district_key)
            elif src == "tba":
                rankings = get_tba(f"district/{district_key}/rankings")
            else:
                raise ValueError(f"Unknown DISTRICT_RANKINGS_SOURCE entry: {src!r} (use 'first' and/or 'tba')")
            break
        except Exception as e:
            last_err = e
            rankings = None
    if rankings is None:
        raise Exception(f"Could not load district rankings for {district_key}: {last_err}")

    con.execute("CREATE TABLE IF NOT EXISTS district_rankings (district_key VARCHAR, team_key VARCHAR, rank INT, points INT, rookie_bonus INT)")
    for ranking in rankings:
        con.execute(
            "INSERT INTO district_rankings (district_key, team_key, rank, points, rookie_bonus) VALUES (?, ?, ?, ?, ?)",
            (district_key, ranking["team_key"], ranking["rank"], ranking["point_total"], ranking["rookie_bonus"]),
        )


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