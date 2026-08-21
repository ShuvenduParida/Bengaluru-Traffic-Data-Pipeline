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

jar_path = (
    "C:/Users/suppo/OneDrive/Desktop/HTML email template test/"
    "Traffic_Pipeline/drivers/redshift-jdbc42-2.2.8.jar"
)

print("JAR path:")
print(jar_path)

print("\nJAR exists:")
print(os.path.exists(jar_path))

if not os.path.exists(jar_path):
    raise FileNotFoundError(
        f"Redshift JDBC driver not found:\n{jar_path}"
    )

spark = (
    SparkSession.builder
    .appName("RedshiftConnectionTest")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.jars", jar_path)
    .config("spark.driver.extraClassPath", jar_path)
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
    .option("driver", "com.amazon.redshift.Driver")
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