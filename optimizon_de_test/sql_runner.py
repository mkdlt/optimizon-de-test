from google.cloud import bigquery

CLEAN_QUERY_FILE_PATH = 'sql/clean_raw_data.sql'
METRICS_QUERY_FILE_PATHS = {
    'sql/grand_totals.sql',
    'sql/metrics_monthly.sql',
    'sql/metrics_yearly.sql'
}

def load_sql(file_path: str, params: dict) -> str:
    with open(file_path, 'r') as f:
        query = f.read()
    for key, value in params.items():
        query = query.replace(f"{{{{{key}}}}}", value)
    return query

def clean_raw_data(raw_table_id: str, clean_table_id: str) -> dict:
    client = bigquery.Client()

    query = load_sql(
        file_path=CLEAN_QUERY_FILE_PATH,
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

    clean_stats = {
        'clean_time': clean_time.total_seconds(),
        'clean_rows': clean_table.num_rows
    }

    return clean_stats

def get_metrics(clean_table_id: str) -> dict:
    client = bigquery.Client()
    metrics = {}

    for file_path in METRICS_QUERY_FILE_PATHS:
        query = load_sql(
            file_path=file_path,
            params={'clean_table_id': clean_table_id}
        )

        job = client.query(query)
        result = dict(next(job.result()))
        metrics.update(result)
    
    return metrics

