import config
import random
from datetime import datetime
from optimizon_de_test.gcs_uploader import upload_csv_to_gcs
from optimizon_de_test.bigquery_loader import load_csv_to_raw_layer
from optimizon_de_test.sql_runner import clean_raw_data

CSV_URL = (
    "https://storage.googleapis.com/nozzle-csv-exports/testing-data/"
    "order_items_data_1_.csv"
)

def main():
    # file_id = generate_file_id()
    file_id = 'order_items_250728_020036_e771'
    gcs_uri = f'gs://{config.GCS_BUCKET}/{file_id}.csv'
    raw_table_id = f'{config.GCP_PROJECT}.raw.{file_id}'
    clean_table_id = f'{config.GCP_PROJECT}.clean.{file_id}'

    print(f'Starting pipeline. Assigned id: {file_id}')

    upload_time = upload_csv_to_gcs(
        csv_url=CSV_URL,
        bucket_name=config.GCS_BUCKET,
        blob_name=f'{file_id}.csv'
    )

    load_time, num_rows = load_csv_to_raw_layer(
        gcs_uri=gcs_uri,
        raw_table_id=raw_table_id
    )

    clean_time = clean_raw_data(
        raw_table_id=raw_table_id,
        clean_table_id=clean_table_id
    )

def generate_file_id() -> str:
    timestamp = datetime.now().strftime('%y%m%d_%H%M%S')
    random_base64 = f'{random.randrange(16**4):04x}'

    return f'order_items_{timestamp}_{random_base64}'

if __name__ == "__main__":
    main()