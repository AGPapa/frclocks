import duckdb
from jinja2 import Environment, FileSystemLoader
import os
import numpy as np
import pandas as pd

def generate_homepage():
    env = Environment(loader=FileSystemLoader('html_templates'))
    template = env.get_template('homepage.html')
    context = {}

    html_content = template.render(**context)

    with open("output_html/homepage/homepage.html", "w") as file:
        file.write(html_content)

def generate_district_page(district_key: str, con: duckdb.DuckDBPyConnection):
    env = Environment(loader=FileSystemLoader('html_templates'))
    template = env.get_template('district.html')

    # Get district data from database
    rankings = con.execute(f"""
        SELECT 
            rank,
            SUBSTRING(team_key, 4) AS team_number,
            team_key,
            event1_points,
            event2_points,
            rookie_bonus,
            total_points,
            lock_status,
            color
        FROM lock_status 
        WHERE district_key = '{district_key}'
        ORDER BY rank
    """).df().to_dict('records')

    # Get district events data
    events = con.execute(f"""
        SELECT 
            event_states.event_key AS key,
            event_states.name,
            event_states.event_state AS status,
            event_points_remaining.team_count,
            event_points_remaining.points_remaining,
            event_states.color
        FROM event_states
        JOIN event_points_remaining ON event_states.event_key = event_points_remaining.event_key
        WHERE event_states.district_key = '{district_key}'
        ORDER BY event_states.start_date, event_states.name
    """).df().to_dict('records')

    # Get district summary stats
    stats = con.execute(f"""
        SELECT 
            district_points_remaining.points_remaining,
            district_lookup.dcmp_capacity,
            district_lookup.display_name
        FROM district_points_remaining
        JOIN district_lookup ON district_points_remaining.district_key = district_lookup.district_key
        WHERE district_points_remaining.district_key = '{district_key}'
    """).df().to_dict('records')[0]

    context = {
        'rankings': rankings,
        'events': events,
        'district_stats': stats
    }

    html_content = template.render(**context)

    with open(f"output_html/districts/{district_key[4:]}.html", "w") as file:
        file.write(html_content)

def generate_event_page(event_key: str, con: duckdb.DuckDBPyConnection):
    env = Environment(loader=FileSystemLoader('html_templates'))
    template = env.get_template('event.html')

    # Get event data
    event = con.execute(f"""
        SELECT 
            event_states.name,
            event_states.event_state AS status,
            event_states.color
        FROM event_states
        WHERE event_states.event_key = '{event_key}'
    """).df().to_dict('records')[0]

    # Get points remaining data
    points_remaining = con.execute(f"""
        SELECT 
            quals_adjusted,
            alliance_selection_points,
            elimination_points,
            award_points,
            points_remaining
        FROM event_points_remaining
        WHERE event_key = '{event_key}'
    """).df().to_dict('records')[0]

    context = {
        'event': event,
        'points_remaining': points_remaining
    }

    html_content = template.render(**context)

    with open(f"output_html/events/{event_key}.html", "w") as file:
        file.write(html_content)

def generate_team_page(team_key: str, con: duckdb.DuckDBPyConnection):
    env = Environment(loader=FileSystemLoader('html_templates'))
    template = env.get_template('team.html')
    lock_status = con.execute(f"""
        SELECT 
            team_key,
            total_inflated_points,
            total_points_to_pass,
            total_points_remaining,
            lock_status
        FROM lock_status
        WHERE team_key = '{team_key}'
    """).df().to_dict('records')[0]

    following_teams = con.execute(f"""
        SELECT 
            following_team_key,
            SUBSTRING(following_team_key, 4) AS following_team_number,
            following_team_rank,
            following_team_points AS inflated_points_total,
            following_team_points_needed_to_pass AS points_to_pass
        FROM following_teams
        WHERE team_key = '{team_key}'
        ORDER BY following_team_rank ASC
    """).df().to_dict('records')

    context = {
        'lock_status': lock_status,
        'following_teams': following_teams
    }

    html_content = template.render(**context)

    with open(f"output_html/teams/{team_key}.html", "w") as file:
        file.write(html_content)


def generate_html(district_key: str, con: duckdb.DuckDBPyConnection):
    generate_homepage()
    generate_district_page(district_key, con)
    events = con.execute(f"SELECT event_key FROM events WHERE district_key = '{district_key}' and event_type = 'District'").df().to_dict('records')
    for event in events:
        generate_event_page(event['event_key'], con)
    teams = con.execute(f"SELECT team_key FROM district_rankings WHERE district_key = '{district_key}'").df().to_dict('records')
    for team in teams:
        generate_team_page(team['team_key'], con)

if __name__ == "__main__":
    con = duckdb.connect()
    generate_html("2024fma", con)