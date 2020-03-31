import boto3
import io
import configparser

config = configparser.ConfigParser()
config.read('config.ini')


s3 = boto3.resource('s3')



google_access_key_id = config['bq_mover']['google_access_key_id']
google_access_key_secret = config['bq_mover']['google_access_key_secret']

project = config['bq_mover']['project']

def get_gcs_objects(google_access_key_id, google_access_key_secret):
        client = boto3.client(
        "s3",
        region_name="auto",
        endpoint_url="https://storage.googleapis.com",
        aws_access_key_id=google_access_key_id,
        aws_secret_access_key=google_access_key_secret,
    )
        response = client.list_objects(Bucket='testbq2s3avro')

        print("Objects:")
        for blob in response["Contents"]:
                print(blob["Key"])

        bucket = s3.Bucket('klfanalytics')
        #for obj in bucket.objects.all():
        #       print(obj.key, obj.last_modified)
        object = s3.Object('klfanalytics', '2020/03/30/a-*.avro')
        f = io.BytesIO()
        client.download_fileobj("testbq2s3avro","2020/03/30/a.avro",f)
        object.put(Body=f.getvalue())

if __name__ == "__main__":

        get_gcs_objects(google_access_key_id,google_access_key_secret)
