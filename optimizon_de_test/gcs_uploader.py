
from time import perf_counter

import requests
from google.cloud import storage


def upload_csv_to_gcs(
        csv_url: str,
        bucket_name: str,
        blob_name: str
    ):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.content_type = 'text/csv'

    start_time = perf_counter()

    print('Connecting to GCS...')
    with requests.get(csv_url, stream=True, timeout=300) as response:
        response.raise_for_status()

        print(f"Started streaming upload from {csv_url}")
        with blob.open("wb") as file:
            for chunk in response.iter_content(chunk_size=(1024**2 * 4)):
                if chunk:
                    file.write(chunk)

    end_time = perf_counter()
    upload_time = end_time - start_time

    print(f"Streamed upload completed: gs://{bucket_name}/{blob_name}")
    print(f"Time elapsed: {upload_time:.2f}s")

    return upload_time
