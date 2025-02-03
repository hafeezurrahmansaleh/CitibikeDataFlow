from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, year, month, dayofmonth, hour, date_format, lit, when, count, avg, udf
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import math
import requests
import json

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Citibike Data Processing") \
    .getOrCreate()
# Define Schema for the Dataset
schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("ended_at", TimestampType(), True),
    StructField("start_station_name", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_name", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("start_lat", DoubleType(), True),
    StructField("start_lng", DoubleType(), True),
    StructField("end_lat", DoubleType(), True),
    StructField("end_lng", DoubleType(), True),
    StructField("member_casual", StringType(), True)
])
# Load Raw Data from S3
raw_data_path = "s3://citibike-df/inbound-data/*.csv"
df = spark.read.schema(schema).option("header", "true").csv(raw_data_path)

# Define Haversine Formula to Calculate Distance
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in kilometers
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

# Register Haversine function as a UDF
haversine_udf = udf(haversine, DoubleType())
# Data Cleaning and Transformation
processed_df = df.withColumnRenamed("member_casual", "user_type") \
    .withColumn("trip_duration_seconds",
                (col("ended_at").cast("long") - col("started_at").cast("long"))) \
    .withColumn("start_year", year(col("started_at"))) \
    .withColumn("start_month", month(col("started_at"))) \
    .withColumn("start_day", dayofmonth(col("started_at"))) \
    .withColumn("start_hour", hour(col("started_at"))) \
    .withColumn("end_year", year(col("ended_at"))) \
    .withColumn("end_month", month(col("ended_at"))) \
    .withColumn("end_day", dayofmonth(col("ended_at"))) \
    .withColumn("end_hour", hour(col("ended_at"))) \
    .withColumn("user_type",
                when(col("user_type") == "member", "Subscriber")
                .otherwise("Customer")) \
    .withColumn("start_station_id", col("start_station_id").cast(StringType())) \
    .withColumn("end_station_id", col("end_station_id").cast(StringType())) \
    .withColumn("start_lat", col("start_lat").cast(DoubleType())) \
    .withColumn("start_lng", col("start_lng").cast(DoubleType())) \
    .withColumn("end_lat", col("end_lat").cast(DoubleType())) \
    .withColumn("end_lng", col("end_lng").cast(DoubleType())) \
    .dropDuplicates(["ride_id"])  # Remove duplicate rows based on ride_id

# Handle Missing Values
processed_df = processed_df.na.fill({
    "start_station_name": "Unknown",
    "start_station_id": "Unknown",
    "end_station_name": "Unknown",
    "end_station_id": "Unknown",
    "start_lat": 0.0,
    "start_lng": 0.0,
    "end_lat": 0.0,
    "end_lng": 0.0
})
# Calculate Trip Distance using Haversine Formula
processed_df = processed_df.withColumn(
    "trip_distance_km",
    haversine_udf(col("start_lat"), col("start_lng"), col("end_lat"), col("end_lng"))
)
# Create Dimension Tables
# 1. Stations Dimension
stations = processed_df.select(
    col("start_station_id").alias("station_id"),
    col("start_station_name").alias("station_name"),
    col("start_lat").alias("latitude"),
    col("start_lng").alias("longitude")
).distinct()
stations.show()
# 2. Users Dimension
users = processed_df.select(
    col("ride_id").alias("user_id"),  # Assuming ride_id can be used as user_id since we don't have the user data
    lit(1990).alias("birth_year"),    # Placeholder for birth year
    lit("Unknown").alias("gender")    # Placeholder for gender
).distinct()

# 3. Time Dimension
# Set legacy time parser policy
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
time_dim = processed_df.select(
    col("started_at").alias("time_id"),
    year(col("started_at")).alias("year"),
    month(col("started_at")).alias("month"),
    dayofmonth(col("started_at")).alias("day"),
    hour(col("started_at")).alias("hour"),
    date_format(col("started_at"), "u").alias("day_of_week"),  # 'u' for day of the week (1 = Monday, 7 = Sunday)
    when(date_format(col("started_at"), "u").isin([6, 7]), True).otherwise(False).alias("is_weekend")
).distinct()
# 4. Bikes Dimension (Placeholder)
bikes = processed_df.select(
    col("ride_id").alias("bike_id"),  # Assuming ride_id can be used as bike_id
    lit("classic").alias("bike_type"),  # Placeholder for bike type
    lit("Unknown").alias("manufacturer"),  # Placeholder for manufacturer
    lit("2020-01-01").alias("purchase_date")  # Placeholder for purchase date
).distinct()
# 5. Weather Dimension (Placeholder)
# Fetch weather data using an OpenWeatherMap API
def fetch_weather(lat, lng, timestamp):
    api_key = "855152b9f44ec9bbf0c3c1f9d2f0b534"
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lng}&dt={timestamp}&appid={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        weather_data = response.json()
        return {
            "temperature_c": weather_data["main"]["temp"] - 273.15,  # Convert Kelvin to Celsius
            "precipitation_mm": weather_data.get("rain", {}).get("1h", 0.0),
            "weather_condition": weather_data["weather"][0]["main"]
        }
    else:
        return {
            "temperature_c": 0.0,
            "precipitation_mm": 0.0,
            "weather_condition": "Unknown"
        }

# Create Weather Dimension
weather_df = processed_df.select(
    col("started_at").alias("time_id"),
    col("start_lat").alias("latitude"),
    col("start_lng").alias("longitude")
).distinct()
weather_df = weather_df.limit(5)
weather_df.show()
# Fetch weather data for each row (this can be optimized further)
weather_data = []
for row in weather_df.collect():
    weather = fetch_weather(row["latitude"], row["longitude"], int(row["time_id"].timestamp()))
    weather_data.append({
        "time_id": row["time_id"],
        "temperature_c": weather["temperature_c"],
        "precipitation_mm": weather["precipitation_mm"],
        "weather_condition": weather["weather_condition"]
    })
weather_dim = spark.createDataFrame(weather_data)
weather_dim.show()
# Prepare Trip Fact Table
trips = processed_df.select(
    col("ride_id").alias("trip_id"),
    col("start_station_id"),
    col("end_station_id"),
    col("started_at").alias("start_time_id"),
    col("ended_at").alias("end_time_id"),
    col("ride_id").alias("user_id"),  # Assuming ride_id can be used as user_id
    col("ride_id").alias("bike_id"),  # Assuming ride_id can be used as bike_id
    col("trip_duration_seconds"),
    col("trip_distance_km"),
    col("user_type")
)
trips.show()
# Save Dimension Tables to S3
dimensions_path = "s3://citibike-df/outbound-data/dimensions/"
stations.write.mode("overwrite").parquet(f"{dimensions_path}stations_dim")
users.write.mode("overwrite").parquet(f"{dimensions_path}users_dim")
time_dim.write.mode("overwrite").parquet(f"{dimensions_path}time_dim")
bikes.write.mode("overwrite").parquet(f"{dimensions_path}bikes_dim")
weather_dim.write.mode("overwrite").parquet(f"{dimensions_path}weather_dim")
# Save Fact Table to S3
fact_path = "s3://citibike-df/outbound-data/fact/"
trips.write.mode("overwrite").parquet(f"{fact_path}trips_fact")
# Stop Spark Session
spark.stop()
job.commit()