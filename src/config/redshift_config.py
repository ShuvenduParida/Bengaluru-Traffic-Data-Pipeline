import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

host = os.getenv("REDSHIFT_HOST")
port = os.getenv("REDSHIFT_PORT")
database = os.getenv("REDSHIFT_DATABASE")
username = os.getenv("REDSHIFT_USER")
password = os.getenv("REDSHIFT_PASSWORD")
aws_access_key = os.getenv("YOUR_AWS_ACCESS_KEY")
aws_secret_key = os.getenv("YOUR_AWS_SECRET_KEY")

jdbc_url = f"jdbc:redshift://{host}:{port}/{database}"

JAR_PATH = r"C:\Users\91738\OneDrive\Desktop\Traffic-Data-Pipeline\drivers\redshift-jdbc42-2.2.8.jar"
jdbc_driver = "com.amazon.redshift.Driver"

TRAFFIC_S3_PATH = (
    "s3a://silver-transformaer-traffic-data/silver_data/traffic/"
)

ROUTES_S3_PATH = (
    "s3a://silver-transformaer-traffic-data/silver_data/routes/"
)


# Redshift COPY uses s3://, NOT s3a://
TRAFFIC_COPY_PATH = (
    "s3://silver-transformaer-traffic-data/silver_data/traffic/"
)

ROUTES_COPY_PATH = (
    "s3://silver-transformaer-traffic-data/silver_data/routes/"
)

REDSHIFT_IAM_ROLE = (
    "arn:aws:iam::864365983876:role/service-role/AmazonRedshift-CommandsAccessRole-20260819T203317"
)