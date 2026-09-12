import time
import os
import redshift_connector
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"
from pyspark.sql import SparkSession
from src.config.redshift_config import (
    host,port,database,username,password,jdbc_url,JAR_PATH, jdbc_driver,aws_access_key,aws_secret_key,
    TRAFFIC_S3_PATH,ROUTES_S3_PATH,TRAFFIC_COPY_PATH,ROUTES_COPY_PATH,REDSHIFT_IAM_ROLE)

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

traffic_df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "bronze.traffic")
    .option("user",username)
    .option("password", password)
    .option("driver", jdbc_driver)
    .load()
)

routes_df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "bronze.routes")
    .option("user",username)
    .option("password", password)
    .option("driver", jdbc_driver)
    .load()
)


traffic_df = traffic_df.drop("temp","realfeel","humidity","rsi_flag","aqi")
# test_df = traffic_df.limit(1000)

# traffic_df = traffic_df.repartition(6)

print("\nWriting Silver traffic data to S3...")

start_time = time.time()

(
    traffic_df.write
    .mode("overwrite")
    .parquet(TRAFFIC_S3_PATH)
)

print("Traffic Parquet written successfully.")


print("\nWriting Silver routes data to S3...")

(
    routes_df.write
    .mode("overwrite")
    .parquet(ROUTES_S3_PATH)
)

print("Routes Parquet written successfully.")
s3_time = time.time() - start_time

print(
    f"\nS3 write completed in "
    f"{round(s3_time, 2)} seconds"
)


print("\nConnecting to Redshift for COPY operation...")
conn = redshift_connector.connect(
    host=host,
    database=database,
    port=port,
    user=username,
    password=password
)

cursor = conn.cursor()


try:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS silver.traffic
        (
            date DATE,
            time TIMESTAMP,
            route_code VARCHAR(100),
            duration INTEGER,
            distance DECIMAL(10,2)
        );
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS silver.routes
        (
            route_code VARCHAR(100),
            label_full VARCHAR(500),
            label_short VARCHAR(200),
            map_link VARCHAR(1000),
            accuweather_station VARCHAR(500)
        );
        """
    )

    print("\nLoading traffic Parquet from S3 to Redshift...")


    traffic_copy_sql = f"""
        COPY silver.traffic
        FROM '{TRAFFIC_COPY_PATH}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        FORMAT AS PARQUET;
    """


    cursor.execute(traffic_copy_sql)

    print("Traffic data loaded successfully.")


    print("\nLoading routes Parquet from S3 to Redshift...")


    routes_copy_sql = f"""
        COPY silver.routes
        FROM '{ROUTES_COPY_PATH}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        FORMAT AS PARQUET;
    """


    cursor.execute(routes_copy_sql)


    print("Routes data loaded successfully.")

    conn.commit()

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM silver.traffic limit 5;
        """
    )

    traffic_count = cursor.fetchone()[0]


    cursor.execute(
        """
        SELECT COUNT(*)
        FROM silver.routes limit 5;
        """
    )

    routes_count = cursor.fetchone()[0]


    print("\n========================================")
    print("SILVER PIPELINE COMPLETED")
    print("========================================")

    print(
        f"silver.traffic rows : {traffic_count}"
    )

    print(
        f"silver.routes rows  : {routes_count}"
    )

    print("========================================")


except Exception as e:

    conn.rollback()

    print("\nSilver Redshift load failed:")
    print(e)

    raise


finally:

    cursor.close()
    conn.close()
    spark.stop()




