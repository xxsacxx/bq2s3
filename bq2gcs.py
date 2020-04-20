
from google.cloud import bigquery
import configparser
from datetime import  datetime,date,timedelta

config = configparser.ConfigParser()
config.read('config.ini')
client = bigquery.Client()

bucket_name = config['bq2gcs']['gcs_bucket']

project = config['bq2gcs']['project']
dataset_id = config['bq2gcs']['dataset_id']
job_config = bigquery.job.ExtractJobConfig()
job_config.destination_format = bigquery.DestinationFormat.AVRO

dataset_ref = client.dataset(dataset_id,project=project)
sdate = date(2020, 3, 25)   # start date
edate = date(2020, 4, 11)   # end date

delta = edate - sdate       # as timedelta
for i in range(delta.days + 1):
    day = sdate + timedelta(days=i)
    
    table_id = config['bq2gcs']['table_name']+"$"+(str(day.year)+str(day.month).zfill(2) 
                +str(day.day).zfill(2))
    destination_uri = "gs://{}/{}".format(bucket_name+'/'+(str(day.year)+'/'+str(day.month).zfill(2) 
                        +'/'+str(day.day).zfill(2)), "data.avro")








    table_ref = dataset_ref.table(table_id)

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        job_config=job_config,
        # Location must match that of the source table.
        location="US",
    )  # API request
    extract_job.result()  # Waits for job to complete.

    print(
        "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
    )
