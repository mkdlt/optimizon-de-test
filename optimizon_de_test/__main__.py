import config
from optimizon_de_test.gcs_uploader import stream_upload_to_gcs
from optimizon_de_test.bigquery_loader import load_csv_to_bigquery

CSV_URL = (
    "https://storage.googleapis.com/nozzle-csv-exports/testing-data/"
    "order_items_data_1_.csv"
)

def main():
    print('Starting pipeline...')

    # gcs_uri = 'gs://optimizon-de-test-data/streaming_test.csv'
    
    gcs_uri = stream_upload_to_gcs(
        csv_url=CSV_URL,
        bucket_name=config.GCS_BUCKET,
        blob_name="streaming_test.csv"
    )

    table_id=f"{config.GCP_PROJECT}.{config.BQ_DATASET}.loading_test"

    load_csv_to_bigquery(
        gcs_uri=gcs_uri,
        table_id=table_id
    )

if __name__ == "__main__":
    main()