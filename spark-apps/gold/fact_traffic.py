from pyspark.sql.functions import col, when, avg, count
from pyspark.sql.types import FloatType, IntegerType
from common import write_to_postgres, write_to_gold

def process_fact_traffic(df_silver, dim_location, dim_weather, dim_owner):

    print("Processing Fact_Traffic with Joins...")
    dim_loc_origin = dim_location.alias("dim_loc_origin")
    dim_loc_dest   = dim_location.alias("dim_loc_dest")

    # Join with dim_road to get road_id
    df_joined = df_silver.join(
        dim_loc_origin,
        (df_silver.road_street == dim_loc_origin.street) & 
        (df_silver.road_district == dim_loc_origin.district) & 
        (df_silver.road_city == dim_loc_origin.city),
        "left"
    ).drop(dim_loc_origin.street).drop(dim_loc_origin.district).drop(dim_loc_origin.city)
    
    df_joined = df_silver.join(
        dim_loc_dest,
        (df_silver.destination_street == dim_loc_dest.street) & 
        (df_silver.destination_district == dim_loc_dest.district) & 
        (df_silver.destination_city == dim_loc_dest.city),
        "left"
    ).drop(dim_loc_dest.street).drop(dim_loc_dest.district).drop(dim_loc_dest.city)
    
    # Join with dim_weather to get weather_id
    df_joined = df_joined.join(
        dim_weather,
        (df_joined.weather_condition == dim_weather.weather_condition) & 
        (df_joined.temperature.cast(FloatType()) == dim_weather.temperature) & 
        (df_joined.humidity.cast(FloatType()) == dim_weather.humidity),
        "left"
    ).drop(dim_weather.weather_condition).drop(dim_weather.temperature).drop(dim_weather.humidity)
    
    # Join with dim_owner to get owner_id
    df_joined = df_joined.join(
        dim_owner,
        (df_joined.email == dim_owner.email),
        "left"
    ).drop(dim_owner.email).drop(dim_owner.phone).drop(dim_owner.owner_name)

    # Select and cast measures
    df_fact = df_joined.select(
        col("timestamp").alias("time_id"),
        col("vehicle_id"),
        col("owner_id"), 
        col("dim_loc_origin.location_id").alias("ori_location_id"),
        col("dim_loc_dest.location_id").alias("des_location_id"),
        col("weather_id"),
        col("speed_kmph").cast(FloatType()),
        col("rpm").cast(IntegerType()),
        col("fuel_level_percentage").cast(FloatType()),
        col("passenger_count").cast(IntegerType()),
        when(col("congestion_level") == "Low", 1)
            .when(col("congestion_level") == "Moderate", 2)
            .when(col("congestion_level") == "High", 3)
            .when(col("congestion_level") == "Heavy", 4)
            .otherwise(0).cast(IntegerType()).alias("congestion_score"
        ),
        col("estimated_delay_minutes").cast(IntegerType()),
        col("eta").alias("destination_eta")
    )

    write_to_gold(df_fact, "fact_traffic", "append")
    write_to_postgres(df_fact, "fact_traffic", "append")
    
    # Aggregation: Hourly Average Speed and Traffic Count per Road
    print("Processing Fact_Traffic Aggregation (Hourly Metrics)...")
    df_agg = df_fact.groupBy("org_location_id", "time_id") \
        .agg(
            avg("speed_kmph").alias("avg_speed"),
            avg("congestion_score").alias("avg_congestion"),
            count("vehicle_id").alias("traffic_count")
        )
    write_to_gold(df_agg, "fact_traffic_hourly_agg", "overwrite")
    write_to_postgres(df_agg, "fact_traffic_hourly_agg", "overwrite")
    
    print(f"Fact_Traffic completed: {df_fact.count()} records, Aggregated: {df_agg.count()} records")

