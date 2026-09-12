import time
import os
import redshift_connector
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col,hour,avg,min,max,count)
from src.config.redshift_config import (
    host,port,database,username,password,jdbc_url,JAR_PATH, jdbc_driver,aws_access_key,aws_secret_key,
    REDSHIFT_IAM_ROLE)

spark = SparkSession(
    SparkSession.builder
    .appName("Traffic_Silver_Transformation")
    .master("local[*]")
    .config("spark.driver.extraClassPath", JAR_PATH)
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.5.0")
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)


TRAFFIC_S3_PATH = (
    f"s3a://silver-transformaer-traffic-data/gold_analysis/daily_route_summary/"
)

HOURLY_S3_PATH = (
    f"s3a://silver-transformaer-traffic-data/gold_analysis/hourly_traffic_summary/"
)

GOLD_TRAFFIC_S3_PATH = (
    f"s3://silver-transformaer-traffic-data/gold_analysis/daily_route_summary/"
)

GOLD_HOURLY_S3_PATH = (
    f"s3://silver-transformaer-traffic-data/gold_analysis/hourly_traffic_summary/"
)


print("\nReading Silver tables from Redshift...")

traffic_df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "silver.traffic")
    .option("user", username)
    .option("password", password)
    .option("driver", jdbc_driver)
    .load()
)

routes_df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "silver.routes")
    .option("user", username)
    .option("password", password)
    .option("driver", jdbc_driver)
    .load()
)


print("\nJoining traffic and route information...")

traffic_with_routes = (
    traffic_df
    .join(
        routes_df,
        traffic_df.route_code == routes_df.route_code,
        "left"
    )
    .select(
        traffic_df.date,
        traffic_df.time,
        traffic_df.route_code,
        routes_df.label_full.alias("route_name"),
        traffic_df.duration,
        traffic_df.distance
    )
)

print("Join completed successfully.")

print("\nCreating gold.daily_route_summary...")

daily_route_summary = (
    traffic_with_routes
    .groupBy(
        "date",
        "route_code",
        "route_name"
    )
    .agg(
        count("*").alias("total_records"),
        avg("duration").cast("double").alias("avg_duration"),
        min("duration").alias("min_duration"),
        max("duration").alias("max_duration"),
        avg("distance").cast("double").alias("avg_distance")
    )
    .select(
        "date",
        "route_code",
        "route_name",
        "total_records",
        "avg_duration",
        "min_duration",
        "max_duration",
        "avg_distance"
    )
)

daily_count = daily_route_summary.count()

print(
    f"Daily route summary rows : {daily_count}"
)

print("\nCreating gold.hourly_traffic_summary...")

hourly_traffic_summary = (
    traffic_with_routes
    .withColumn(
        "hour_of_day",
        hour(col("time"))
    )
    .groupBy(
        "route_code",
        "route_name",
        "hour_of_day"
    )
    .agg(
        count("*").alias("total_records"),
        avg("duration").cast("double").alias("avg_duration"),
        min("duration").alias("min_duration"),
        max("duration").alias("max_duration")
    )
    .select(
        "route_code",
        "route_name",
        "hour_of_day",
        "total_records",
        "avg_duration",
        "min_duration",
        "max_duration"
    )
)

hourly_count = hourly_traffic_summary.count()

print(
    f"Hourly traffic summary rows : {hourly_count}"
)

print("\nWriting Gold data to S3...")

start_time = time.time()


(
    daily_route_summary
    .write
    .mode("overwrite")
    .parquet(TRAFFIC_S3_PATH)
)

print("Daily route summary Parquet written successfully.")


(
    hourly_traffic_summary
    .write
    .mode("overwrite")
    .parquet(HOURLY_S3_PATH)
)

print("Hourly traffic summary Parquet written successfully.")


s3_time = time.time() - start_time

print(
    f"S3 write completed in {s3_time:.2f} seconds"
)

print("\nConnecting to Redshift for COPY...")

conn = redshift_connector.connect(
    host=host,
    port=port,
    database=database,
    user=username,
    password=password
)

cursor = conn.cursor()

print("Redshift connection successful.")

cursor.execute("""
    CREATE SCHEMA IF NOT EXISTS gold;
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS gold.daily_route_summary (
        date DATE,
        route_code VARCHAR(100),
        route_name VARCHAR(500),
        total_records BIGINT,
        avg_duration DOUBLE PRECISION,
        min_duration INTEGER,
        max_duration INTEGER,
        avg_distance DOUBLE PRECISION
    );
""")


cursor.execute("""
    CREATE TABLE IF NOT EXISTS gold.hourly_traffic_summary (
        route_code VARCHAR(100),
        route_name VARCHAR(500),
        hour_of_day INTEGER,
        total_records BIGINT,
        avg_duration DOUBLE PRECISION,
        min_duration INTEGER,
        max_duration INTEGER
    );
""")

print("\nLoading daily route summary into Redshift...")

cursor.execute(f"""
    COPY gold.daily_route_summary
    FROM '{GOLD_TRAFFIC_S3_PATH}'
    IAM_ROLE '{REDSHIFT_IAM_ROLE}'
    FORMAT AS PARQUET;
""")

print(
    "Daily route summary loaded successfully."
)

print("\nLoading hourly traffic summary into Redshift...")

cursor.execute(f"""
    COPY gold.hourly_traffic_summary
    FROM '{GOLD_HOURLY_S3_PATH}'
    IAM_ROLE '{REDSHIFT_IAM_ROLE}'
    FORMAT AS PARQUET;
""")

print(
    "Hourly traffic summary loaded successfully."
)

conn.commit()

print("\nValidating Gold tables...")

cursor.execute("""
    SELECT COUNT(*)
    FROM gold.daily_route_summary limit 5;
""")

daily_redshift_count = cursor.fetchone()[0]


cursor.execute("""
    SELECT COUNT(*)
    FROM gold.hourly_traffic_summary limit 5;
""")

hourly_redshift_count = cursor.fetchone()[0]


print(
    f"gold.daily_route_summary rows  : "
    f"{daily_redshift_count}"
)

print(
    f"gold.hourly_traffic_summary rows : "
    f"{hourly_redshift_count}"
)

cursor.close()
conn.close()

print("GOLD TRANSFORMATION COMPLETED SUCCESSFULLY")


spark.stop()