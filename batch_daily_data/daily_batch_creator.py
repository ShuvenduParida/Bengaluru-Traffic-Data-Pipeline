import io
from datetime import datetime, timedelta

import boto3
import pandas as pd
import requests




GITHUB_CSV_URL = (
    "https://raw.githubusercontent.com/"
    "thecont1/traffic-monitor-lizard/main/"
    "data/csv-traffic-bangalore.csv"
)


S3_BUCKET = "bengaluru-traffic-daily-raw"

S3_BASE_PATH = "daily_raw/traffic"

print("\nDownloading latest traffic data from GitHub...")

response = requests.get(
    GITHUB_CSV_URL,
    timeout=60
)

response.raise_for_status()

content_type = response.headers.get("Content-Type", "")

if "text/html" in content_type:
    raise SystemExit("ERROR: GitHub returned HTML instead of CSV.")

print("Source CSV downloaded successfully.")

print("Downloaded size:", len(response.text))
print("First 200 characters:")
print(response.text[:200])

df = pd.read_csv(
    io.StringIO(response.text),
    sep=",",
    engine="python"
)


df["date"] = pd.to_datetime(
    df["date"],
    errors="coerce"
)

yesterday = (
    datetime.now().date() - timedelta(days=1)
)

yesterday = pd.Timestamp(yesterday)

print(
    f"Batch date: "
    f"{yesterday.strftime('%Y-%m-%d')}"
)

daily_df = df[
    df["date"].dt.date == yesterday.date()
].copy()

if daily_df.empty:

    print(
        f"\nNo data found for "
        f"{yesterday.strftime('%Y-%m-%d')}"
    )

    raise SystemExit(
        "Daily batch was not created."
    )


print(
    f"Records for {yesterday.strftime('%Y-%m-%d')}: "
    f"{len(daily_df)}"
)

daily_df["date"] = (
    daily_df["date"]
    .dt.strftime("%Y-%m-%d")
)

csv_buffer = io.StringIO()

daily_df.to_csv(
    csv_buffer,
    index=False
)

batch_date = yesterday.strftime(
    "%Y-%m-%d"
)

s3_key = (
    f"{S3_BASE_PATH}/"
    f"date={batch_date}/"
    f"traffic.csv"
)

print(
    f"\nS3 destination:"
)

print(
    f"s3://{S3_BUCKET}/{s3_key}"
)

print("\nConnecting to S3...")

s3 = boto3.client("s3")

print("S3 connection successful.")

try:

    s3.head_object(
        Bucket=S3_BUCKET,
        Key=s3_key
    )

    print(
        f"\nBatch already exists for "
        f"{batch_date}."
    )

    print(
        "Skipping upload to prevent duplicate batch."
    )

    raise SystemExit(
        "Daily batch already processed."
    )

except s3.exceptions.ClientError as error:

    error_code = error.response[
        "Error"
    ]["Code"]

    if error_code != "404":
        raise

print(
    "\nUploading daily batch to S3..."
)

s3.put_object(
    Bucket=S3_BUCKET,
    Key=s3_key,
    Body=csv_buffer.getvalue(),
    ContentType="text/csv"
)


# ============================================================
# 15. SUCCESS
# ============================================================

print("\n==========================================")
print("DAILY BATCH CREATED SUCCESSFULLY")
print("==========================================")

print(
    f"Batch date      : {batch_date}"
)

print(
    f"Records         : {len(daily_df)}"
)

print(
    f"S3 location     : "
    f"s3://{S3_BUCKET}/{s3_key}"
)

print(
    "Status          : SUCCESS"
)

print("==========================================")