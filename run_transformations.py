import duckdb
import os


def run_transformations(con: duckdb.DuckDBPyConnection, mode: str):
    if mode == 'dcmp':
        transforms_folder = 'dcmp_transforms'
    elif mode == 'dcmp_divisions':
        transforms_folder = 'dcmp_divisions_transforms'
    else:
        transforms_folder = 'transforms'

    sql_files = sorted([f for f in os.listdir(transforms_folder) if f.endswith('.sql')])

    for sql_file in sql_files:
        with open(os.path.join(transforms_folder, sql_file), 'r') as file:
            sql_statement = file.read()
            con.execute(sql_statement)