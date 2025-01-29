# FRC Locks

This program creates the html files for the FRC Locks website. It pulls data from the TBA API, runs a series of SQL transformations, and then creates the html files.

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

Create a ".env" file in the root directory with a "TBA_AUTH_KEY" variable. You can get a key from https://www.thebluealliance.com/account.

Requirements can be installed with `pip install -r requirements.txt`. The code can be run with `python main.py`.

## Deployment

The deployment is done using the `lambda_deploy_script.sh` file. It creates a function zip file that can be uploaded to an AWS Lambda function, along with a layer zip file for the dependencies.
