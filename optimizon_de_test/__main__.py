import config
from optimizon_de_test.gcs_uploader import stream_upload_to_gcs

stream_upload_to_gcs(
    csv_url="https://storage.googleapis.com/nozzle-csv-exports/testing-data/order_items_data_1_.csv",
    bucket_name=config.GCS_BUCKET,
    blob_name="streaming_test.csv"
)