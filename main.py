import duckdb
import json
import time

from collect_data import collect_data
from run_transformations import run_transformations
from load_lookup_tables import load_lookup_tables
from generate_html import generate_html

def lambda_handler(event, context):
    try:
        # Get district_key from event or use default
        district_key = event.get('district_key', '2024fma')
        mode = event.get('mode', 'district')

        # Initialize DuckDB connection
        con = duckdb.connect(':memory:')  # Using in-memory database for Lambda

        # Run pipeline
        start_time = time.time()
        load_lookup_tables(con)
        load_lookup_tables_time = time.time()
        print(f"Time to load lookup tables: {load_lookup_tables_time - start_time:.2f} seconds")

        collect_data(district_key, con)
        collect_data_time = time.time()
        print(f"Time to collect data: {collect_data_time - load_lookup_tables_time:.2f} seconds")

        run_transformations(con, mode)
        run_transformations_time = time.time()
        print(f"Time to run transformations: {run_transformations_time - collect_data_time:.2f} seconds")

        generate_html(district_key, con, mode)
        generate_html_time = time.time()
        print(f"Time to generate HTML: {generate_html_time - run_transformations_time:.2f} seconds")

        # Clean up
        con.close()

        return {
            'statusCode': 200,
            'body': json.dumps({'message': f'Successfully processed district {district_key}'})
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

# Keep the main() function for local testing
def main():
    test_event = {'district_key': '2025isr', 'mode': 'dcmp'}
    print(lambda_handler(test_event, None))

if __name__ == "__main__":
    main()