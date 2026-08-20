import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()


host = os.getenv("REDSHIFT_HOST")
port = os.getenv("REDSHIFT_PORT")
database = os.getenv("REDSHIFT_DATABASE")
username = os.getenv("REDSHIFT_USER")
password = os.getenv("REDSHIFT_PASSWORD")


jdbc_url = f"jdbc:redshift://{host}:{port}/{database}"

spark = (
    SparkSession.builder
    .appName("RedshiftConnectionTest")
    .getOrCreate()
)

print("\nConnecting to Redshift...\n")


df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "daily_traffic_data.location")
    .option("user", username)
    .option("password", password)
    .option("driver", "com.amazon.redshift.jdbc.Driver")
    .load()
)


print("Connection successful!")
print("\nSchema:")
df.printSchema()

print("\nFirst 10 rows:")
df.show(10, truncate=False)

print("\nTotal rows:")
print(df.count())




spark.stop()