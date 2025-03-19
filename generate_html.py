import duckdb
from jinja2 import Environment, FileSystemLoader
import os
from dotenv import load_dotenv
import boto3
from io import StringIO

load_dotenv()
ENV = os.getenv('ENV')
DIR = "output_html"
S3_BUCKET = os.getenv('S3_BUCKET')
GA_TRACKING_ID = os.getenv('GA_TRACKING_ID')

def write_file(content: str, path: str):
    if ENV == "PROD":
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=path,
            Body=content,
            ContentType='text/html'
        )
    else:
        os.makedirs(os.path.dirname(f"{DIR}/{path}"), exist_ok=True)
        with open(f"{DIR}/{path}", "w") as file:
            file.write(content)

def duckdb_result_to_dict(query: str, con: duckdb.DuckDBPyConnection):
    result = con.execute(query)
    columns = [col[0] for col in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]

def write_static_files():
    if ENV == "PROD":
        s3 = boto3.client('s3')
        with open("static/css/style.css", "r") as file:
            css_content = file.read()
            s3.put_object(
                Bucket=S3_BUCKET,
                Key="css/style.css",
                Body=css_content,
                ContentType='text/css'
            )
        with open("static/images/FIRST_Horz_RGB.png", "rb") as file:
            image_content = file.read()
            s3.put_object(
                Bucket=S3_BUCKET,
                Key="images/FIRST_Horz_RGB.png",
                Body=image_content,
                ContentType='image/png'
            )
        with open("static/images/andymark_logo.png", "rb") as file:
            image_content = file.read()
            s3.put_object(
                Bucket=S3_BUCKET,
                Key="images/andymark_logo.png",
                Body=image_content,
                ContentType='image/png'
            )
        with open("static/images/district_key.png", "rb") as file:
            image_content = file.read()
            s3.put_object(
                Bucket=S3_BUCKET,
                Key="images/district_key.png",
                Body=image_content,
                ContentType='image/png'
            )
        with open("static/images/lock.svg", "rb") as file:
            image_content = file.read()
            s3.put_object(
                Bucket=S3_BUCKET,
                Key="images/lock.svg",
                Body=image_content,
                ContentType='image/svg+xml'
            )
        with open("static/images/lock.ico", "rb") as file:
            image_content = file.read()
            s3.put_object(
                Bucket=S3_BUCKET,
                Key="images/lock.ico",
                Body=image_content,
                ContentType='image/x-icon'
            )

def generate_homepage():
    env = Environment(loader=FileSystemLoader('html_templates'))
    template = env.get_template('index.html')
    context = {
        'env': ENV,
        'ga_tracking_id': GA_TRACKING_ID
    }

    html_content = template.render(**context)
    write_file(html_content, "index.html")

def generate_district_page(district_key: str, con: duckdb.DuckDBPyConnection):
    env = Environment(loader=FileSystemLoader('html_templates'))
    template = env.get_template('district.html')

    # Get district data from database
    rankings = duckdb_result_to_dict(f"""
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
    """, con)

    # Get district events data
    events = duckdb_result_to_dict(f"""
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
    """, con)

    # Get district summary stats
    stats = duckdb_result_to_dict(f"""
        SELECT 
            district_points_remaining.points_remaining,
            district_lookup.dcmp_capacity,
            district_lookup.display_name
        FROM district_points_remaining
        JOIN district_lookup ON district_points_remaining.district_key = district_lookup.district_key
        WHERE district_points_remaining.district_key = '{district_key}'
    """, con)[0]

    context = {
        'rankings': rankings,
        'events': events,
        'district_stats': stats,
        'env': ENV,
        'ga_tracking_id': GA_TRACKING_ID
    }

    html_content = template.render(**context)
    write_file(html_content, f"districts/{district_key[4:]}.html")

def generate_event_page(event_key: str, con: duckdb.DuckDBPyConnection):
    env = Environment(loader=FileSystemLoader('html_templates'))
    template = env.get_template('event.html')

    # Get event data
    event = duckdb_result_to_dict(f"""
        SELECT 
            event_states.name,
            event_states.event_state AS status,
            event_states.color
        FROM event_states
        WHERE event_states.event_key = '{event_key}'
    """, con)[0]

    # Get points remaining data
    points_remaining = duckdb_result_to_dict(f"""
        SELECT 
            quals_adjusted,
            alliance_selection_points,
            elimination_points,
            award_points,
            points_remaining
        FROM event_points_remaining
        WHERE event_key = '{event_key}'
    """, con)[0]

    context = {
        'event': event,
        'points_remaining': points_remaining,
        'env': ENV,
        'ga_tracking_id': GA_TRACKING_ID
    }

    html_content = template.render(**context)
    write_file(html_content, f"events/{event_key}.html")

def generate_team_page(team_key: str, con: duckdb.DuckDBPyConnection):
    env = Environment(loader=FileSystemLoader('html_templates'))
    template = env.get_template('team.html')
    lock_status = duckdb_result_to_dict(f"""
        SELECT 
            team_key,
            total_inflated_points,
            teams_to_pass,
            total_teams_that_can_pass,
            total_points_to_pass,
            total_points_remaining,
            lock_status
        FROM lock_status
        WHERE team_key = '{team_key}'
    """, con)[0]

    following_teams = duckdb_result_to_dict(f"""
        SELECT 
            following_team_key,
            SUBSTRING(following_team_key, 4) AS following_team_number,
            following_team_rank,
            following_team_inflated_points AS inflated_points_total,
            following_team_max_possible_points AS max_points,
            COALESCE(CAST(following_team_points_needed_to_pass AS VARCHAR), '-') AS points_to_pass,
            following_team_color AS color
        FROM following_teams
        WHERE team_key = '{team_key}'
        ORDER BY following_team_rank ASC
    """, con)

    context = {
        'lock_status': lock_status,
        'following_teams': following_teams,
        'env': ENV,
        'ga_tracking_id': GA_TRACKING_ID
    }

    # Only generate team pages if needed
    if lock_status['lock_status'] not in ['-', '0%', 'Impact']:
        html_content = template.render(**context)
        write_file(html_content, f"teams/{team_key}.html")


def generate_html(district_key: str, con: duckdb.DuckDBPyConnection):
    write_static_files()
    generate_homepage()
    generate_district_page(district_key, con)

    events = duckdb_result_to_dict(f"SELECT event_key FROM events WHERE district_key = '{district_key}' and event_type = 'District'", con)
    for event in events:
        generate_event_page(event['event_key'], con)

    teams = duckdb_result_to_dict(f"SELECT team_key FROM district_rankings WHERE district_key = '{district_key}'", con)
    for team in teams:
        generate_team_page(team['team_key'], con)

if __name__ == "__main__":
    con = duckdb.connect()
    generate_html("2024fma", con)