import duckdb

from collect_data import collect_data
from run_transformations import run_transformations
from load_lookup_tables import load_lookup_tables
from generate_html import generate_html


def main():
    district_key = '2024fma'
    con = duckdb.connect()
    load_lookup_tables(con)
    collect_data(district_key, con)
    run_transformations(con)
    generate_html(district_key, con)
    con.close()

if __name__ == "__main__":
    main()