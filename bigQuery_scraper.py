from google.cloud import bigquery

client = bigquery.Client()
bucket_name = 'my-bucket'

destination_uri = "gs://{}/{}".format(bucket_name, "shakespeare.csv.gz")
dataset_ref = bigquery.DatasetReference(project, dataset_id)
table_ref = dataset_ref.table("shakespeare")
job_config = bigquery.job.ExtractJobConfig()
job_config.compression = bigquery.Compression.GZIP

extract_job = client.extract_table(
    table_ref,
    destination_uri,
    # Location must match that of the source table.
    location="US",
    job_config=job_config,
)  # API request
extract_job.result()  # Waits for job to complete.
