
from google.cloud import bigquery
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
client = bigquery.Client()

bucket_name = 'testbq2s3avro'

project = config['bq2gcs']['project']
dataset_id = config['bq2gcs']['dataset_id']
table_id = config['bq2gcs']['table_id']



destination_uri = "gs://{}/{}".format(bucket_name+'/2020/03/30', "a.avro")


dataset_ref = client.dataset(dataset_id,project=project)
table_ref = dataset_ref.table(table_id)

extract_job = client.extract_table(
    table_ref,
    destination_uri,
    # Location must match that of the source table.
    location="US",
)  # API request
extract_job.result()  # Waits for job to complete.

print(
    "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
)
