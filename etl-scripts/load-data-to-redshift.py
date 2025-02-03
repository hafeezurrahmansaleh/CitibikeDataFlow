import boto3
import psycopg2
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    REDSHIFT_CONFIG = {
        "host": "citibike-wrkgrp.700159367355.us-east-1.redshift-serverless.amazonaws.com",
        "port": 5439,
        "database": "dev",
        "user": "admin",
        "password": "Admin1234",
        "iam_role": "arn:aws:iam::700159367355:role/service-role/AmazonRedshift-CommandsAccessRole-20250202T044958"
    }

    S3_PATHS = {
        "stations_dim": "s3://citibike-df/outbound-data/dimensions/stations_dim/",
        "users_dim": "s3://citibike-df/outbound-data/dimensions/users_dim/",
        "time_dim": "s3://citibike-df/outbound-data/dimensions/time_dim/",
        "bikes_dim": "s3://citibike-df/outbound-data/dimensions/bikes_dim/",
        "weather_dim": "s3://citibike-df/outbound-data/dimensions/weather_dim/",
        "trips_fact": "s3://citibike-df/outbound-data/fact/trips_fact/"
    }

    conn = None
    cursor = None

    def run_copy_command(table_name, s3_path):
        sql = f"""
            COPY {table_name}
            FROM '{s3_path}'
            IAM_ROLE '{REDSHIFT_CONFIG["iam_role"]}'
            FORMAT PARQUET
            COMPUPDATE OFF
            STATUPDATE OFF
        """
        cursor.execute(sql)
        logger.info(f"Loaded data into {table_name}")

    try:
        conn = psycopg2.connect(
            host=REDSHIFT_CONFIG["host"],
            port=REDSHIFT_CONFIG["port"],
            dbname=REDSHIFT_CONFIG["database"],
            user=REDSHIFT_CONFIG["user"],
            password=REDSHIFT_CONFIG["password"]
        )
        cursor = conn.cursor()
        logger.info("Connected to Redshift")

        # Load Dimensions
        run_copy_command("stations_dim", S3_PATHS["stations_dim"])
        run_copy_command("users_dim", S3_PATHS["users_dim"])
        run_copy_command("time_dim", S3_PATHS["time_dim"])
        run_copy_command("bikes_dim", S3_PATHS["bikes_dim"])
        run_copy_command("weather_dim", S3_PATHS["weather_dim"])

        # Load Fact Table
        run_copy_command("trips_fact", S3_PATHS["trips_fact"])

        conn.commit()
        return {"statusCode": 200, "body": "Data loaded successfully!"}

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        if conn:
            conn.rollback()
        return {"statusCode": 500, "body": f"Error: {str(e)}"}

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()