import requests
import boto3


def lambda_handler(event, context):
    url = "https://s3.amazonaws.com/tripdata/202401-citibike-tripdata.csv.zip"
    response = requests.get(url)

    s3 = boto3.client('s3')
    bucket_name = "citibike-raw-data-bucket"
    file_name = "202401-citibike-tripdata.zip"

    s3.put_object(Bucket=bucket_name, Key=file_name, Body=response.content)
    return {"statusCode": 200, "body": "Data ingested successfully"}
