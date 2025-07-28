
from time import perf_counter
from datetime import timedelta
import csv
import re

import requests
from google.cloud import storage

STARTS_WITH_ORDER_ID = re.compile(r"^[\"]?[a-f0-9€]{4}")

def upload_csv_to_gcs(
        csv_url: str,
        bucket_name: str,
        file_id: str,
        buffer_length: int = 50000
    ):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    keep_blob = bucket.blob(f'{file_id}.csv')
    drop_blob = bucket.blob(f'{file_id}_discarded_rows.csv')

    keep_blob.content_type = 'text/csv'
    drop_blob.content_type = 'text/csv'

    start_time = perf_counter()

    print('Connecting to GCS...')
    with requests.get(csv_url, stream=True, timeout=300) as response:
        response.raise_for_status()
        response.encoding = 'utf-8'

        print(f"Started streaming upload from {csv_url}")

        with keep_blob.open("w") as keep_stream, \
            drop_blob.open("w") as drop_stream:

            total_lines, total_keep, total_drop = 0, 0, 0
            keep_count, drop_count = 0, 0
            keep_buffer, drop_buffer = [], []
            split_line_buffer = ''

            for line in response.iter_lines(decode_unicode=True):
                total_lines += 1
                if not is_empty_line(line):
                    if STARTS_WITH_ORDER_ID.match(line):
                        if split_line_buffer:
                            keep_buffer.append(split_line_buffer + '\n')
                            keep_count += 1
                            total_keep += 1
                        split_line_buffer = line
                    else:
                        split_line_buffer += ' ' + line
                    
                    if keep_count >= buffer_length:
                        keep_stream.writelines(keep_buffer)
                        keep_buffer.clear()
                        keep_count = 0
                else:
                    drop_buffer.append(line + '\n')
                    drop_count += 1
                    total_drop += 1
                    if drop_count >= buffer_length:
                        drop_stream.writelines(drop_buffer)
                        drop_buffer.clear()
                        drop_count = 0

            if split_line_buffer:
                keep_buffer.append(split_line_buffer + '\n')
                keep_count += 1
                total_keep += 1
        
            if keep_buffer:
                keep_stream.writelines(keep_buffer)

            if drop_buffer:
                drop_stream.writelines(drop_buffer)

    end_time = perf_counter()
    upload_time = end_time - start_time

    print(f"Streamed upload completed: gs://{bucket_name}/{file_id}.csv")
    print(f"Upload time: {timedelta(seconds=upload_time)}")
    print(f"Kept/merged lines: {total_keep}")
    print(f"Dropped lines: {total_drop}")
    print(f"Total lines processed: {total_lines}")

    upload_stats = {
        'upload_time': upload_time,
        'kept_lines': total_keep,
        'dropped_lines': total_drop
    }

    return upload_stats

def is_empty_line(line):
    valid_line_re = re.compile(r"[a-zA-Z0-9]+")
    close_quote_re = re.compile(r'^[^a-zA-Z0-9,]{1,3}"$')

    cleaned = line.strip().strip("\"',")
    if valid_line_re.search(cleaned) or close_quote_re.search(line):
        return False
    return True


