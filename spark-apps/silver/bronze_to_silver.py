from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, to_date, from_json, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    LongType, BooleanType, ArrayType, DecimalType
)

# MinIO Configuration
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"

# Paths
BRONZE_PATH = "s3a://bronze/raw/*/*.json"  # Reading all raw data
SILVER_PATH = "s3a://silver/traffic_data"

def create_spark_session():
    return SparkSession.builder \
        .appName("Traffic-Bronze-To-Silver") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.read.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.commit.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") \
        .config("spark.hadoop.fs.s3a.assumed.role.session.duration", "1800") \
        .getOrCreate()

def get_schema():
    # Define schema based on README description to ensure correct types
    return StructType([
        StructField("vehicle_id", StringType(), True),
        StructField("owner", StructType([
            StructField("name", StringType(), True),
            StructField("license_number", StringType(), True),
            StructField("contact_info", StructType([
                StructField("phone", StringType(), True),
                StructField("email", StringType(), True)
            ]), True)
        ]), True),
        StructField("speed_kmph", DecimalType(10, 2), True),
        StructField("road", StructType([
            StructField("street", StringType(), True),
            StructField("district", StringType(), True),
            StructField("city", StringType(), True)
        ]), True),
        StructField("timestamp", StringType(), True),
        StructField("vehicle_size", StructType([
            StructField("length_meters", DecimalType(10, 2), True),
            StructField("width_meters", DecimalType(10, 2), True),
            StructField("height_meters", DecimalType(10, 2), True)
        ]), True),
        StructField("vehicle_type", StringType(), True),
        StructField("vehicle_classification", StringType(), True),
        StructField("coordinates", StructType([
            StructField("latitude", DecimalType(10, 2), True),
            StructField("longitude", DecimalType(10, 2), True)
        ]), True),
        StructField("engine_status", StructType([
            StructField("is_running", BooleanType(), True),
            StructField("rpm", LongType(), True),
            StructField("oil_pressure", StringType(), True)
        ]), True),
        StructField("fuel_level_percentage", LongType(), True),
        StructField("passenger_count", LongType(), True),
        StructField("internal_temperature_celsius", DecimalType(10, 2), True),
        StructField("weather_condition", StructType([
            StructField("temperature_celsius", DecimalType(10, 2), True),
            StructField("humidity_percentage", DecimalType(10, 2), True),
            StructField("condition", StringType(), True)
        ]), True),
        StructField("traffic_status", StructType([
            StructField("congestion_level", StringType(), True),
            StructField("estimated_delay_minutes", LongType(), True)
        ]), True),
        StructField("alerts", ArrayType(StructType([
            StructField("type", StringType(), True),
            StructField("description", StringType(), True),
            StructField("severity", StringType(), True),
            StructField("timestamp", StringType(), True)
        ])), True)
    ])

def transform_data(df):
    return df.select(
        col("vehicle_id"),
        col("owner.name").alias("owner_name"),
        col("owner.license_number").alias("license_number"),
        col("vehicle_type"),
        col("vehicle_classification"),
        col("speed_kmph"),
        # Road Info
        col("road.street").alias("road_street"),
        col("road.district").alias("road_district"),
        col("road.city").alias("road_city"),
        # Time (Convert here)
        to_timestamp(col("timestamp")).alias("timestamp"),
        to_date(to_timestamp(col("timestamp"))).alias("date"),
        # Vehicle Size
        col("vehicle_size.length_meters").alias("vehicle_length"),
        col("vehicle_size.width_meters").alias("vehicle_width"),
        col("vehicle_size.height_meters").alias("vehicle_height"),
        # Coordinates
        col("coordinates.latitude"),
        col("coordinates.longitude"),
        # Engine
        col("engine_status.rpm").alias("rpm"),
        col("engine_status.oil_pressure").alias("oil_pressure"),
        col("engine_status.is_running").alias("is_running"),
        col("fuel_level_percentage"),
        # Weather
        col("weather_condition.condition").alias("weather_condition"),
        col("weather_condition.temperature_celsius").alias("temperature"),
        col("weather_condition.humidity_percentage").alias("humidity"),
        # Traffic
        col("traffic_status.congestion_level").alias("congestion_level"),
        col("traffic_status.estimated_delay_minutes").alias("estimated_delay_minutes"),
        # Misc
        col("passenger_count"),
        col("internal_temperature_celsius"),
        # Alerts
        col("alerts") 
    )

def get_validation_condition():
    return [
        col("vehicle_id").isNotNull(),
        col("timestamp").isNotNull(),
        col("road_street").isNotNull(),
        col("road_district").isNotNull(),

        (col("speed_kmph").isNull() | ((col("speed_kmph") >= 0) & (col("speed_kmph") < 300))),
        
        (col("vehicle_length").isNull() | (col("vehicle_length") >= 0)),
        (col("vehicle_width").isNull() | (col("vehicle_width") >= 0)),
        (col("vehicle_height").isNull() | (col("vehicle_height") >= 0)),

        (col("fuel_level_percentage").isNull() | (col("fuel_level_percentage").between(0, 100))),

        (col("passenger_count").isNull() | (col("passenger_count") >= 0)),
        (col("rpm").isNull() | (col("rpm") >= 0)),

        (col("latitude").isNull() | (col("latitude").between(-90, 90))) &
        (col("longitude").isNull() | (col("longitude").between(-180, 180))),
    ]

def clean_data(df):
    return df.dropDuplicates(["vehicle_id", "timestamp"])

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Reading data from Bronze layer...")
    schema = get_schema()
    df = spark.read.schema(schema).json(BRONZE_PATH)

    # 1. Transformation (Flattening)
    print("Transforming and flattening data...")

    df_flat = transform_data(df)

    # 2. Cleaning & Validation
    print("Validating flattened data...")
    validation_conditions = get_validation_condition()
    
    # Initialize with the first condition
    combined_condition = validation_conditions[0]
    for condition in validation_conditions[1:]:
        combined_condition = combined_condition & condition
        
    df_valid = df_flat.filter(combined_condition)
    df_invalid = df_flat.filter(~combined_condition)

    if df_invalid.count() > 0:
        print(f"Found {df_invalid.count()} invalid records. Writing to bad_data...")
        df_invalid.write.mode("append").json(f"{SILVER_PATH}_bad_data")
    
    # 3. Clean duplicates
    print("Removing duplicates...")
    df_clean = clean_data(df_valid)

    # 4. Write data with Optimized Partitioning
    print("Writing data to Silver layer (Parquet)...")
    
    # Optimization: Sort within partitions to cluster data by district physically
    # This helps downstream jobs that filter by district even without physical district partitioning
    df_clean \
        .sortWithinPartitions("road_district", "timestamp") \
        .write \
        .mode("append") \
        .partitionBy("date") \
        .parquet(SILVER_PATH)
    
    print(f"Silver layer processing completed. Data written to {SILVER_PATH}")
    spark.stop()

if __name__ == "__main__":
    main()
