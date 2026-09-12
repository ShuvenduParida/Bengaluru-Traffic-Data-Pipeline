from pyspark.sql import SparkSession

from pyspark.sql.functions import col, to_date, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType
)

from src.config.redshift_config import (
username,password,jdbc_url,JAR_PATH, jdbc_driver)

ROUTES_S3_PATH = (
    "s3a://bengaluru-traffic-daily-raw/daily_raw/routes/"
)

TRAFFIC_S3_PATH = (
    "s3a://bengaluru-traffic-daily-raw/daily_raw/traffic/"
)

routes_schema = StructType([
    StructField("route_code", StringType(), True),
    StructField("label_full", StringType(), True),
    StructField("label_short", StringType(), True),
    StructField("map_link", StringType(), True),
    StructField("accuweather_station", StringType(), True)
])


traffic_schema = StructType([
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("route_code", StringType(), True),
    StructField("duration", IntegerType(), True),
    StructField("distance", DecimalType(10, 2), True),
    StructField("temp", DecimalType(5, 2), True),
    StructField("realfeel", DecimalType(5, 2), True),
    StructField("humidity", DecimalType(5, 2), True),
    StructField("rsi_flag", StringType(), True),
    StructField("aqi", DecimalType(8, 2), True)
])

spark = (
    SparkSession.builder
    .appName("Traffic_Bronze_Leader")
    .master("local[*]")
    .config("spark.driver.extraClassPath", JAR_PATH)
    .getOrCreate()
)

SOURCE_TABLES = [

    {
        "table_name": "routes",
        "s3_path": ROUTES_S3_PATH,
        "schema": routes_schema
    },

    {
        "table_name": "traffic",
        "s3_path": TRAFFIC_S3_PATH,
        "schema": traffic_schema
    }

]
for source in SOURCE_TABLES:

    table_name = source["table_name"]
    s3_path = source["s3_path"]
    schema = source["schema"]


    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load(s3_path)
    )

    if table_name == "traffic":

        df = (
            df
            .withColumn(
                "date",
                to_date(col("date"))
            )
            .withColumn(
                "time",
                to_timestamp(
                    col("time"),
                    "HH:mm:ss"
                )
            )
        )


    print(f"\nRaw S3 data loaded: {table_name}")

    df.printSchema()

    source_count = df.count()

    print(f"Source row count: {source_count}")


    bronze_table = f"bronze.{table_name}"

    print(f"\nWriting to Bronze: {bronze_table}")

    df = df.repartition(4)

    print(
        "Spark partitions:",
        df.rdd.getNumPartitions()
    )

    (
        df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", bronze_table)
        .option("user", username)
        .option("password", password)
        .option("driver", jdbc_driver)
        .option("batchsize", 5000)
        .mode("overwrite")
        .save()
    )

spark.stop()
