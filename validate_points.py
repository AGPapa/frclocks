import duckdb


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
        if qual_points[0]['sum'] != 0:
            return True
        else:
            print(f"Event {event_key} has 0 qualification points, but should have at least 1.")
            return False
    elif event_state[0]['event_state'] == 'Elims 1 to 7':
        selection_points = duckdb_result_to_dict(f"SELECT SUM(selection_points) AS sum FROM event_points WHERE event_key = '{event_key}'", con)
        if selection_points[0]['sum'] != 0:
            return True
        else:
            print(f"Event {event_key} has 0 selection points, but should have at least 1.")
            return False


    elim_points = [('Elims 8', 39), ('Elims 9', 78), ('Elims 10', 99), ('Elims 11', 120), ('Elims 12', 141), ('Elims 13', 159), ('Finals', 180)]
    multiplier = 3 if mode == 'dcmp' else 1
    for state, points in elim_points:
        if event_state[0]['event_state'] == state:
            elim_points = duckdb_result_to_dict(f"SELECT SUM(elim_points) AS sum FROM event_points WHERE event_key = '{event_key}'", con)
            if elim_points[0]['sum'] >= points * multiplier:
                return True
            else:
                print(f"Event {event_key} has {elim_points[0]['sum']} elimination points, but should have at least {points * multiplier}.")
                return False

    return True

def validate_events(con: duckdb.DuckDBPyConnection, mode: str) -> bool:
    events = duckdb_result_to_dict("SELECT event_key FROM event_states", con)
    for event in events:
        if not validate_event_points(con, event['event_key'], mode):
            return False
    return True