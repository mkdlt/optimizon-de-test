from google.cloud import bigquery

def load_csv_to_raw_layer(gcs_uri: str, raw_table_id: str):
    client = bigquery.Client()

    schema = [
        bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("purchased_at", "STRING"),
        bigquery.SchemaField("purchased_date", "STRING"),
        bigquery.SchemaField("purchased_month_ended", "STRING"),
        bigquery.SchemaField("order_item_id", "STRING"),
        bigquery.SchemaField("sku", "STRING"),
        bigquery.SchemaField("product_title", "STRING"),
        bigquery.SchemaField("product_name_full", "STRING"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("item_price", "STRING"),
        bigquery.SchemaField("item_tax", "STRING"),
        bigquery.SchemaField("shipping_price", "STRING"),
        bigquery.SchemaField("shipping_tax", "STRING"),
        bigquery.SchemaField("gift_wrap_price", "STRING"),
        bigquery.SchemaField("gift_wrap_tax", "STRING"),
        bigquery.SchemaField("item_promo_discount", "STRING"),
        bigquery.SchemaField("shipment_promo_discount", "STRING"),
        bigquery.SchemaField("ship_service_level", "STRING"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        max_bad_records=1000
    )

    print(f"Loading {gcs_uri} into {raw_table_id}")

    job = client.load_table_from_uri(
        gcs_uri, raw_table_id, job_config=job_config
    )

    job.result()

    destination_table = client.get_table(raw_table_id)

    load_time = job.ended - job.started
    num_rows = destination_table.num_rows
    
    print(f'Loaded {destination_table.num_rows} rows')
    print(f'Loading time: {load_time}s')

    return load_time, num_rows

