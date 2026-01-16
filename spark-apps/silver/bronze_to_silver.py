from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_timestamp, current_timestamp
from config import MINIO_CONF
from schema_definition import BRONZE_TRAFFIC_SCHEMA

def get_spark_session():
    return SparkSession.builder \
        .appName("BronzeToSilver") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def process_silver():
    spark = get_spark_session()
    
    # 1. Read Bronze Data
    df_raw = spark.read.schema(BRONZE_TRAFFIC_SCHEMA).json(MINIO_CONF["bronze_path"])

    # 2. Xử lý Bảng Fact Telemetry (Dữ liệu chính để vẽ biểu đồ line chart, gauge trong PowerBI)
    fact_telemetry = df_raw.select(
        "vehicle_id",
        to_timestamp("timestamp").alias("event_time"),
        col("speed_kmph"),
        col("road.street").alias("street"),
        col("road.district").alias("district"),
        col("coordinates.latitude").alias("lat"),
        col("coordinates.longitude").alias("lon"),
        col("traffic_status.congestion_level").alias("congestion_level"),
        current_timestamp().alias("processed_at")
    ).dropDuplicates(["vehicle_id", "event_time"])

    # 3. Xử lý Bảng Alerts (Để làm bảng chi tiết các vi phạm/cảnh báo)
    fact_alerts = df_raw.select(
        "vehicle_id",
        to_timestamp("timestamp").alias("event_time"),
        explode("alerts").alias("alert")
    ).select(
        "vehicle_id",
        "event_time",
        col("alert.type").alias("alert_type"),
        col("alert.severity").alias("alert_severity"),
        col("alert.description").alias("description")
    )

    # 4. Write to Silver (MinIO) dưới dạng Parquet
    # Phân vùng theo district để PowerBI filter nhanh hơn
    fact_telemetry.write.mode("overwrite") \
        .partitionBy("district") \
        .parquet(f"{MINIO_CONF['silver_path']}fact_telemetry/")

    fact_alerts.write.mode("overwrite") \
        .parquet(f"{MINIO_CONF['silver_path']}fact_alerts/")

    print("Success: Bronze to Silver processed.")

if __name__ == "__main__":
    process_silver()