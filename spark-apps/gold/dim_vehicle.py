from pyspark.sql.functions import col
from pyspark.sql.types import FloatType
from common import write_to_postgres, write_to_gold

def process_dim_vehicle(df_silver):
    """Process dim_vehicle from cached silver DataFrame"""
    print("Processing Dim_Vehicle...")
    dim_vehicle = df_silver.select(
        col("vehicle_id"),
        col("owner_name"),
        col("license_number"),
        col("vehicle_type"),
        col("vehicle_classification"),
        col("vehicle_length").cast(FloatType()),
        col("vehicle_width").cast(FloatType()),
        col("vehicle_height").cast(FloatType())
    ).distinct()
    
    write_to_gold(dim_vehicle, "dim_vehicle", "overwrite")
    write_to_postgres(dim_vehicle, "dim_vehicle", "overwrite")
    print(f"Dim_Vehicle completed: {dim_vehicle.count()} records")

