import duckdb


# Expected rows in `district_rankings` (FIRST district standings size, 2026 season).
# Three teams (9684, 10960, 10924) may be absent when sourcing from TBA or sync lag;
# for their districts we allow count in [expected - 1, expected].
EXPECTED_DISTRICT_TEAM_COUNT_2026 = {
    "2026ca": 296,
    "2026fch": 118,
    "2026fim": 531,
    "2026fin": 69,
    "2026fit": 181,
    "2026fma": 143,
    "2026fnc": 90,
    "2026fsc": 40,
    "2026isr": 70,
    "2026ne": 200,
    "2026ont": 120,
    "2026pch": 75,
    "2026pnw": 126,
    "2026win": 71,
}

# Districts where we accept one fewer team (9684 / 10960 / 10924 may be missing).
_DISTRICT_TEAM_COUNT_SLACK = {
    "2026fch": 1,  # 9684
    "2026fnc": 1,  # 10960
    "2026ont": 1,  # 10924
}


def duckdb_result_to_dict(query: str, con: duckdb.DuckDBPyConnection):
    result = con.execute(query)
    columns = [col[0] for col in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]

    
def validate_event_points(con: duckdb.DuckDBPyConnection, event_key: str, mode: str) -> bool:
    event_state = duckdb_result_to_dict(f"SELECT event_state FROM event_states WHERE event_key = '{event_key}'", con)
    if event_state[0]['event_state'] in ['Pre-Event', 'Qualifications']:
        return True
    elif event_state[0]['event_state'] == 'Selections':
        qual_points = duckdb_result_to_dict(f"SELECT SUM(qual_points) AS sum FROM event_points WHERE event_key = '{event_key}'", con)
        elim_sum = duckdb_result_to_dict(f"SELECT SUM(elim_points) AS sum FROM event_points WHERE event_key = '{event_key}'", con)
        if qual_points[0]['sum'] != 0 and elim_sum[0]['sum'] == 0:
            return True
        else:
            print(f"Event {event_key} has {qual_points[0]['sum']} qualification points, but should have at least 1 and {elim_sum[0]['sum']} elimination points, but should be 0.")
            return False
    elif event_state[0]['event_state'] == 'Elims 1 to 7':
        qual_points = duckdb_result_to_dict(f"SELECT SUM(qual_points) AS sum FROM event_points WHERE event_key = '{event_key}'", con)
        selection_points = duckdb_result_to_dict(f"SELECT SUM(selection_points) AS sum FROM event_points WHERE event_key = '{event_key}'", con)
        elim_sum = duckdb_result_to_dict(f"SELECT SUM(elim_points) AS sum FROM event_points WHERE event_key = '{event_key}'", con)

        if selection_points[0]['sum'] != 0 and qual_points[0]['sum'] != 0 and elim_sum[0]['sum'] == 0:
            return True
        else:
            print(f"Event {event_key} has {selection_points[0]['sum']} selection points, but should have at least 1 and {qual_points[0]['sum']} qualification points, but should be at least 1 and {elim_sum[0]['sum']} elimination points, but should be 0.")
            return False


    elim_points = [('Elims 8', 39), ('Elims 9', 78), ('Elims 10', 99), ('Elims 11', 120), ('Elims 12', 141), ('Elims 13', 159), ('Finals', 180), ('Awards', 210)]
    if mode == 'district_regions':
        # TODO: Improve validation
        qual_points = duckdb_result_to_dict(f"SELECT SUM(qual_points) AS sum FROM event_points WHERE event_key = '{event_key}'", con)
        selection_points = duckdb_result_to_dict(f"SELECT SUM(selection_points) AS sum FROM event_points WHERE event_key = '{event_key}'", con)
        if qual_points[0]['sum'] != 0 and selection_points[0]['sum'] != 0:
            return True
        else:
            print(f"Event {event_key} has {qual_points[0]['sum']} qualification points, but should be at least 1 and {selection_points[0]['sum']} selection points, but should be at least 1.")
            return False
    multiplier = 3 if mode in ['dcmp', 'dcmp_divisions'] else 1
    for state, points in elim_points:
        if event_state[0]['event_state'] == state:
            qual_points = duckdb_result_to_dict(f"SELECT SUM(qual_points) AS sum FROM event_points WHERE event_key = '{event_key}'", con)
            selection_points = duckdb_result_to_dict(f"SELECT SUM(selection_points) AS sum FROM event_points WHERE event_key = '{event_key}'", con)
            elim_sum = duckdb_result_to_dict(f"SELECT SUM(elim_points) AS sum FROM event_points WHERE event_key = '{event_key}'", con)
            if elim_sum[0]['sum'] >= points * multiplier and qual_points[0]['sum'] != 0 and selection_points[0]['sum'] != 0:
                return True
            else:
                print(f"Event {event_key} has {elim_sum[0]['sum']} elimination points, but should have at least {points * multiplier} and {qual_points[0]['sum']} qualification points, but should be at least 1 and {selection_points[0]['sum']} selection points, but should be at least 1.")
                return False

    return True


def validate_district_team_count(con: duckdb.DuckDBPyConnection, mode: str) -> bool:
    if mode not in ("district", "district_regions"):
        return True
    keys = duckdb_result_to_dict(
        "SELECT DISTINCT district_key AS k FROM district_rankings", con
    )
    if len(keys) != 1:
        return True
    district_key = keys[0]["k"]
    expected = EXPECTED_DISTRICT_TEAM_COUNT_2026.get(district_key)
    if expected is None:
        return True
    row = duckdb_result_to_dict(
        f"SELECT COUNT(*) AS n FROM district_rankings WHERE district_key = '{district_key}'",
        con,
    )
    actual = row[0]["n"]
    slack = _DISTRICT_TEAM_COUNT_SLACK.get(district_key, 0)
    lo, hi = expected - slack, expected
    if lo <= actual <= hi:
        return True
    print(
        f"District {district_key} has {actual} teams in district_rankings, "
        f"expected between {lo} and {hi} inclusive (target {expected}, slack {slack})."
    )
    return False


def validate_events(con: duckdb.DuckDBPyConnection, mode: str) -> bool:
    events = duckdb_result_to_dict("SELECT event_key FROM event_states", con)
    for event in events:
        if not validate_event_points(con, event["event_key"], mode):
            return False
    if not validate_district_team_count(con, mode):
        return False
    return True