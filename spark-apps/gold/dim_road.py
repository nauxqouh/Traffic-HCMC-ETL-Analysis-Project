from pyspark.sql.functions import monotonically_increasing_id
from common import write_to_postgres, write_to_gold

def process_dim_road(df_silver):
    print("Processing Dim_Road...")
    dim_road = df_silver.select(
        "road_street",
        "road_district",
        "road_city"
    ).distinct().withColumn("road_id", monotonically_increasing_id())

    write_to_gold(dim_road, "dim_road", "overwrite")
    write_to_postgres(dim_road, "dim_road", "overwrite")
    print(f"Dim_Road completed: {dim_road.count()} records")
    
    return dim_road

