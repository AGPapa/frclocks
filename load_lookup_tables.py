import csv
import duckdb

def load_qual_lookup_table(con: duckdb.DuckDBPyConnection):
    con.execute("""
        CREATE TABLE IF NOT EXISTS qual_points_lookup AS
        SELECT
            CAST(event_size AS INT) as event_size,
            CAST(team_rank AS INT) as team_rank,
            CAST(qual_points AS INT) as qual_points
        FROM read_csv_auto('lookups/qual_points_lookup.csv')
    """)

def load_district_lookup_table(con: duckdb.DuckDBPyConnection):
    con.execute("""
        CREATE TABLE IF NOT EXISTS district_lookup AS
        SELECT
            district_key,
            display_name,
            dcmp_name,
            CAST(dcmp_capacity AS INT) as dcmp_capacity,
            CAST(wcmp_capacity AS INT) as wcmp_capacity,
            CAST(dcmp_impact_awards AS INT) as dcmp_impact_awards,
            CAST(dcmp_ei_awards AS INT) as dcmp_ei_awards,
            CAST(dcmp_ras_awards AS INT) as dcmp_ras_awards
        FROM read_csv_auto('lookups/district_lookup.csv')
    """)

def load_points_adjustments(con: duckdb.DuckDBPyConnection):
    con.execute("""
        CREATE TABLE IF NOT EXISTS points_adjustments AS
        SELECT
            district_key,
            team_key,
            CAST(points_adjustment AS INT) as points_adjustment
        FROM read_csv_auto('lookups/points_adjustments.csv')
    """)

def load_prequalified_teams(con: duckdb.DuckDBPyConnection):
    con.execute("""
        CREATE TABLE IF NOT EXISTS prequalified_teams AS
        SELECT
            district_key,
            team_key
        FROM read_csv_auto('lookups/prequalified_teams.csv')
    """)

def load_lookup_tables(con: duckdb.DuckDBPyConnection):
    load_qual_lookup_table(con)
    load_district_lookup_table(con)
    load_points_adjustments(con)
    load_prequalified_teams(con)