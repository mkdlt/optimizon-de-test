import config
from optimizon_de_test.gcs_uploader import stream_upload_to_gcs

CSV_URL = "https://storage.googleapis.com/nozzle-csv-exports/testing-data/" \
    "order_items_data_1_.csv"

def main():
    print('Starting pipeline...')
    stream_upload_to_gcs(
        csv_url=CSV_URL,
        bucket_name=config.GCS_BUCKET,
        blob_name="streaming_test.csv"
    )

if __name__ == "__main__":
    main()