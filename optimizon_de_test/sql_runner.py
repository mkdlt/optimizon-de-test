from google.cloud import bigquery

def load_sql(file_path: str, params: dict) -> str:
    with open(file_path, 'r') as f:
        query = f.read()
    for key, value in params.items():
        query = query.replace(f"{{{{{key}}}}}", value)
    return query

def clean_raw_data(raw_table_id: str, clean_table_id: str):
    client = bigquery.Client()

    query = load_sql(
        file_path='sql/clean_raw_data.sql',
        params={
            'raw_table_id': raw_table_id,
            'clean_table_id': clean_table_id
        }
    )

    print(f'Cleaning raw data and writing to: {clean_table_id}')

    job = client.query(query)
    job.result()

    clean_table = client.get_table(clean_table_id)
    clean_time = job.ended - job.started

    print(f'Wrote {clean_table.num_rows} rows')
    print(f'Cleaning time: {clean_time}')

    return clean_time

