from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col, desc, sum, max, round
from delta import *


def save_to_gold():
    # Ініціалізація SparkSession
    builder = SparkSession.builder \
        .appName("SaveToGold") \
        .master("spark://spark:7077") \
        .config("spark.sql.warehouse.dir", "/opt/prefect/spark-warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    # Читання даних із Silver
    silver_df = spark.readStream.format("delta") \
        .option("readChangeFeed", "true") \
        .load("/opt/prefect/spark-warehouse/delta_silver")

    # Підрахунок кількості знімків за камерами та солами
    agg_sol_cam_gold = silver_df \
        .groupBy("camera_name", "sol", "earth_date") \
        .agg(count("id").alias("photo_count"))

    # Розрахунок середньої кількості знімків на сол для активних камер
    avg_photos_per_sol_gold = silver_df \
        .groupBy("camera_name") \
        .agg(round((count("id") / max("sol")), 2).alias("avg_photos_per_sol"))
    
    # Визначення найбільш активних періодів місії (за кількістю знімків на день)
    active_periods_gold = silver_df \
        .groupBy("sol", "earth_date") \
        .agg(count("id").alias("total_photos"))
    
    # Запис агрегованих даних у Gold таблиці
    query1 = agg_sol_cam_gold.writeStream.format("delta") \
        .outputMode("complete") \
        .option("checkpointLocation", "/tmp/checkpoints_agg_sol_cam_gold") \
        .trigger(once=True) \
        .toTable("agg_sol_cam_gold")
    
    query1.awaitTermination()
    
    query2 = avg_photos_per_sol_gold.writeStream.format("delta") \
        .outputMode("complete") \
        .option("checkpointLocation", "/tmp/checkpoints_avg_photos_per_sol_gold") \
        .trigger(once=True) \
        .toTable("avg_photos_per_sol_gold")
    
    query2.awaitTermination()

    query3 = active_periods_gold.writeStream.format("delta") \
        .outputMode("complete") \
        .option("checkpointLocation", "/tmp/checkpoints_active_periods_gold") \
        .trigger(once=True) \
        .toTable("active_periods_gold")
    
    query3.awaitTermination()

    # export даних в CSV
    df1 = spark.read.parquet("/opt/prefect/spark-warehouse/agg_sol_cam_gold")
    df1.toPandas().to_csv("/opt/prefect/spark-warehouse/result/agg_sol_cam_gold.csv")

    df2 = spark.read.parquet("/opt/prefect/spark-warehouse/active_periods_gold")
    df2.toPandas().to_csv("/opt/prefect/spark-warehouse/result/active_periods_gold.csv")

if __name__ == "__main__":
    save_to_gold()

