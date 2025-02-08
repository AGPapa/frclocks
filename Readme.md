# FRC Locks

This program creates the html files for the FRC Locks website. It pulls data from [The Blue Alliance](https://www.thebluealliance.com/) API, runs a series of SQL transformations, and then creates the html files.

## Pipeline

The pipeline is run by the `main.py` file. It is designed to be run as a lambda function, but can also be run locally.

It runs the following steps:

1. Load lookup tables
2. Collect data from TBA API
3. Run transformations using SQL
4. Generate HTML

The final HTML is intended to be served as static files from an S3 bucket.

## Data Structure

The data structure of the SQL database is defined in `data_structure.txt`.

## Development

1. Have Python 3+ (can be in a docker container)
2. Create a `.env` file in the root directory based on `.env.example`, and set the following variables:
    - `TBA_AUTH_KEY`: Your TBA API key from https://www.thebluealliance.com/account.
    - `ENV`: Set to `DEV` for local development.
3. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```
4. Run the script:
   ```sh
   python main.py
   ```
   The generated HTML files will be saved in the `output_html` directory.

## Deployment

The deployment is done using the `lambda_deploy_script.sh` file. It creates a function zip file that can be uploaded to an AWS Lambda function, along with a layer zip file for the dependencies.
