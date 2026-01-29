from pyspark.sql.functions import monotonically_increasing_id, col, lit, concat_ws
from common import write_to_postgres, write_to_gold

def process_dim_location(df_silver):
    print("Processing Dim_Location...")
    df_location = df_silver.select(
        col("road_street").alias("street"),
        col("road_district").alias("district"),
        col("road_city").alias("city"),
        col("latitude"),
        col("longitude"),
        lit('00700').alias("postal_code"),
        lit("Vietnam").alias("country"),
        concat_ws(", ", col("latitude"), col("longitude")
                      ).alias("geospatial_coordinates")
    ).distinct()
    
    # df_loc_des = df_silver.select(
    #     col("destination_street").alias("street"),
    #     col("destination_district").alias("district"),
    #     col("destination_city").alias("city")
    # )
    dim_location = df_location.withColumn("location_id", monotonically_increasing_id())
    write_to_gold(dim_location, "dim_location", "overwrite")
    write_to_postgres(dim_location, "dim_location", "overwrite")
    print(f"Dim_Road completed: {dim_location.count()} records")
    
    return dim_location

