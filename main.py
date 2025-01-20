import duckdb
import json

from collect_data import collect_data
from run_transformations import run_transformations
from load_lookup_tables import load_lookup_tables
from generate_html import generate_html

def lambda_handler(event, context):
    try:
        # Get district_key from event or use default
        district_key = event.get('district_key', '2024fma')

        # Initialize DuckDB connection
        con = duckdb.connect(':memory:')  # Using in-memory database for Lambda

        # Run pipeline
        load_lookup_tables(con)
        collect_data(district_key, con)
        run_transformations(con)
        generate_html(district_key, con)

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
    test_event = {'district_key': '2024fma'}
    print(lambda_handler(test_event, None))

if __name__ == "__main__":
    main()