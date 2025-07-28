# optimizon-de-test

## Overview
This is a simple CLI tool for processing sales data CSVs. Once provided with a URL to a CSV, this tool will do a streaming upload to Google Cloud Storage, import the data to BigQuery, run some cleaning and aggregation queries, and return a few metrics about the data.

## Requirements
- A Google Cloud Platform account with a designated:
  - Project
  - Google Cloud Storage bucket
  - BigQuery dataset with two schemas: `raw` and `clean`
  - Service account with GCS and BQ APIs enabled + the required permissions
- A `service_account_key.json` file at the top level for the above service account
- A `.env` file at the top level with the variables `GCP_PROJECT`, `GCS_BUCKET`, `BQ_DATASET` (set to the names you chose for those resources), and `GOOGLE_APPLICATION_CREDENTIALS="service_account_key.json"`

## How to run
1. Clone the repository
2. Add the files mentioned above
3. Run `uv venv`, activate the venv, then run `uv sync`
4. Run `python -m optimizon_de_test <CSV_URL>`

## Notes
- I used GCP mainly because I really wanted to use a data warehouse and BigQuery has a pretty good free tier (10 GB of storage, 1 TB of data processed)
- The plan was to get the data into BigQuery as soon as possible and do all of the data manipulation there -- load everything as a string then do some regex to extract timestamps, floats etc. This worked pretty well for the first test CSV, but the second CSV has a lot of garbage lines that prevent it from being loaded as is into BigQuery.
- It seemed like a waste to write the whole file locally, so I just refactored the streaming GCS upload I already had to go line by line instead of in 4 MB chunks. I then had to implement a buffer system so we didn't have to write to GCS after each line.
- Soon after, I noticed that the second CSV has some unquoted newlines in the middle of rows, so I had to implement a buffer for that too.
- All in all, I think I spent most of my time iterating on the cleaning process: regexes, garbage line detection, etc. I spent a few hours spinning my wheels because I didn't notice that I was running into encoding errors with the line iteration thing. Turns out values like `JamÃ³n ibÃ©rico` were not intentional.


