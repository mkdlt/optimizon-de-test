import config
import random
from datetime import datetime
from optimizon_de_test.gcs_uploader import stream_upload_to_gcs
from optimizon_de_test.bigquery_loader import load_csv_to_raw_layer
from optimizon_de_test.sql_runner import clean_raw_data, get_metrics

CSV_URL = (
    "https://storage.googleapis.com/nozzle-csv-exports/testing-data/"
    "order_items_data_1_.csv"
)

def main():
    print('Starting pipeline...')

    file_id = generate_file_id()

    # gcs_uri = 'gs://optimizon-de-test-data/streaming_test.csv'
    
    gcs_uri = stream_upload_to_gcs(
        csv_url=CSV_URL,
        bucket_name=config.GCS_BUCKET,
        blob_name=f"{file_id}.csv"
    )

    raw_table_id=f"{config.GCP_PROJECT}.raw.{file_id}"

    load_csv_to_raw_layer(
        gcs_uri=gcs_uri,
        raw_table_id=raw_table_id
    )

def generate_file_id() -> str:
    timestamp = datetime.now().strftime('%y%m%d%H%M%S')
    random_base64 = f'{random.randrange(16**4):04x}'

    return f'sales_data_{timestamp}_{random_base64}'


if __name__ == "__main__":
    main()