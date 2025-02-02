import requests
import boto3
import zipfile
import io
import os


def lambda_handler(event, context):
    url = "https://s3.amazonaws.com/tripdata/202401-citibike-tripdata.csv.zip"
    response = requests.get(url)

    # Initialize S3 client
    s3 = boto3.client('s3')
    bucket_name = "citibike-raw-data-bucket"
    zip_file_name = "202401-citibike-tripdata.zip"
    csv_file_name = "202401-citibike-tripdata.csv"

    # Upload ZIP file to S3
    s3.put_object(Bucket=bucket_name, Key=zip_file_name, Body=response.content)

    # Unzip the file in memory
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        for file_info in z.infolist():
            if file_info.filename.endswith('.csv'):
                extracted_data = z.read(file_info.filename)

                # Upload the extracted CSV file to S3
                s3.put_object(Bucket=bucket_name, Key=csv_file_name, Body=extracted_data)

    # Optionally, delete the ZIP file from S3 (if not needed)
    s3.delete_object(Bucket=bucket_name, Key=zip_file_name)

    return {"statusCode": 200, "body": "Data ingested and unzipped successfully"}
