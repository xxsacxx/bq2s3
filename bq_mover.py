import boto3
import io
import configparser
from datetime import datetime


config = configparser.ConfigParser()
config.read('config.ini')


s3 = boto3.resource('s3')



google_access_key_id = config['bq_mover']['google_access_key_id']
google_access_key_secret = config['bq_mover']['google_access_key_secret']
gcs_bucket = config['bq_mover']['gcs_bucket']
aws_bucket = config['bq_mover']['aws_bucket']
project = config['bq_mover']['project']

def get_gcs_objects(google_access_key_id, google_access_key_secret,gcs_bucket,aws_bucket):
        client = boto3.client(
        "s3",
        region_name="auto",
        endpoint_url="https://storage.googleapis.com",
        aws_access_key_id=google_access_key_id,
        aws_secret_access_key=google_access_key_secret,
    )
        response = client.list_objects(Bucket=gcs_bucket)

        print("Objects:")
        for blob in response["Contents"]:
                print(blob["Key"])

     

        object = s3.Object(aws_bucket, str(datetime.today().strftime('%Y/%m/%d'))+'/a.avro')
        f = io.BytesIO()
        client.download_fileobj(gcs_bucket,str(datetime.today().strftime('%Y/%m/%d'))+"/a.avro",f)
        object.put(Body=f.getvalue())

if __name__ == "__main__":

        get_gcs_objects(google_access_key_id,google_access_key_secret,gcs_bucket,aws_bucket)
