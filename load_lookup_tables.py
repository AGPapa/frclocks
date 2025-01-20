import csv
import duckdb

def load_qual_lookup_table(con: duckdb.DuckDBPyConnection):
    with open('lookups/qual_points_lookup.csv', 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        header = next(csvreader)  # Skip the header row
        con.execute("CREATE TABLE IF NOT EXISTS qual_points_lookup (event_size INT, team_rank INT, qual_points INT)")
        for row in csvreader:
            event_size, team_rank, qual_points = row
            con.execute(
                "INSERT INTO qual_points_lookup (event_size, team_rank, qual_points) VALUES (?, ?, ?)",
                (int(event_size), int(team_rank), int(qual_points))
            )

def load_district_lookup_table(con: duckdb.DuckDBPyConnection):
    with open('lookups/district_lookup.csv', 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        header = next(csvreader)  # Skip the header row
        con.execute("CREATE TABLE IF NOT EXISTS district_lookup (district_key VARCHAR, display_name VARCHAR, dcmp_capacity INT)")
        for row in csvreader:
            district_key, display_name, dcmp_capacity = row
            con.execute(
                "INSERT INTO district_lookup (district_key, display_name, dcmp_capacity) VALUES (?, ?, ?)",
                (district_key, display_name, int(dcmp_capacity))
            )

def load_lookup_tables(con: duckdb.DuckDBPyConnection):
    load_qual_lookup_table(con)
    load_district_lookup_table(con)
