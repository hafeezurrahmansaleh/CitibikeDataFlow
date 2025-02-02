from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, count, avg, lit, round

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CitiBikeDataETL") \
    .getOrCreate()

# Load raw data from S3
raw_data_path = "s3://citibike-raw-data-bucket/202401-citibike-tripdata.csv"
raw_df = spark.read.csv(raw_data_path, header=True, inferSchema=True)

# Transformation 1: Filter trips longer than 1 minute and valid data
filtered_df = raw_df.filter((col("tripduration") > 60) & (col("starttime").isNotNull()))

# Transformation 2: Add Year and Month columns
filtered_df = filtered_df.withColumn("year", year(col("starttime"))) \
                         .withColumn("month", month(col("starttime")))

# Transformation 3: Create aggregated data for monthly usage by station
monthly_station_agg = filtered_df.groupBy("start_station_id", "year", "month") \
                                 .agg(count("*").alias("trip_count"),
                                      avg("tripduration").alias("avg_trip_duration")) \
                                 .withColumn("aggregation_type", lit("monthly"))

# Transformation 4: Create aggregated data for top 10 stations with the most trips
top_stations = filtered_df.groupBy("start_station_id", "start_station_name") \
                          .agg(count("*").alias("total_trips")) \
                          .orderBy(col("total_trips").desc()) \
                          .limit(10) \
                          .withColumn("aggregation_type", lit("top_stations"))

# Combine all summary tables
summary_data = monthly_station_agg.union(top_stations)

# Write transformed data directly to Redshift
summary_data.write \
    .format("jdbc") \
    .option("url", "jdbc:redshift://<your-cluster-endpoint>:5439/citibike") \
    .option("dbtable", "summary_table") \
    .option("user", "<your-username>") \
    .option("password", "<your-password>") \
    .mode("append") \
    .save()

# Write transformed data back to S3 in Parquet format
transformed_data_path = "s3://citibike-transformed-data-bucket/"
filtered_df.write.mode("overwrite").parquet(transformed_data_path)

# Load transactional data directly to Redshift
filtered_df.write \
    .format("jdbc") \
    .option("url", "jdbc:redshift://<your-cluster-endpoint>:5439/citibike") \
    .option("dbtable", "trips_table") \
    .option("user", "<your-username>") \
    .option("password", "<your-password>") \
    .mode("append") \
    .save()
