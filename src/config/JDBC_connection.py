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

JAR_PATH = r"C:\Users\91738\OneDrive\Desktop\Traffic-Data-Pipeline\drivers\redshift-jdbc42-2.2.8.jar"
jdbc_driver = "com.amazon.redshift.Driver"


print("JAR path:")
print(JAR_PATH)

print("\nJAR exists:")
print(os.path.exists(JAR_PATH))


if not os.path.exists(JAR_PATH):
    raise FileNotFoundError(
        f"Redshift JDBC JAR not found:\n{JAR_PATH}"
    )




spark = (
    SparkSession.builder
    .appName("RedshiftConnectionTest")
    .master("local[*]")
    .config(
        "spark.driver.extraClassPath",
        JAR_PATH
    )
    .getOrCreate()
)


print("\nSpark started successfully!")
print("Connecting to Redshift...\n")




df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "daily_traffic_data.location")
    .option("user", username)
    .option("password", password)
    .option("driver", jdbc_driver)
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