from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from delta import *


def save_to_silver():
    # Ініціалізація SparkSession
    builder = SparkSession.builder \
        .appName("SaveToSilver") \
        .master("spark://spark:7077") \
        .config("spark.sql.warehouse.dir", "/opt/prefect/spark-warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    # Визначення схеми
    schema = StructType([
        StructField("id", IntegerType()),
        StructField("sol", IntegerType()),
        StructField("camera_name", StringType()),
        StructField("img_src", StringType()),
        StructField("earth_date", TimestampType()),
        StructField("rover_name", StringType())
    ])

    table_name = "delta_silver"

    # Перевірка існування таблиці Delta
    DeltaTable.createIfNotExists(spark) \
        .tableName(table_name) \
        .property("delta.enableChangeDataFeed", "true") \
        .addColumns(schema) \
        .execute()

    # Читання даних з Bronze
    bronze_df = spark.readStream.format("delta") \
        .option("readChangeFeed", "true") \
        .load("/opt/prefect/spark-warehouse/delta_bronze")

    # Залишаємо лише потрібні поля
    silver_df = bronze_df.select(["id", "sol", "camera_name", "img_src", "earth_date", "rover_name"])

    # Видалення дублікатів
    silver_df = silver_df.na.drop()

    # Робимо приведення типів
    silver_df = silver_df \
        .withColumn("id", col("id").cast(IntegerType())) \
        .withColumn("earth_date", to_timestamp(col("earth_date"), "yyyy-MM-dd")) \
        .withColumn("sol", col("sol").cast(IntegerType())) \
        .withColumn("camera_name", col("camera_name").cast(StringType())) \
        .withColumn("img_src", col("img_src").cast(StringType())) \
        .withColumn("rover_name", col("rover_name").cast(StringType()))

    # Лишаємо лише перші 250 солів
    silver_df = silver_df \
        .filter(col("sol") <= 250)

    # Запис у Silver таблицю
    query = silver_df.writeStream.format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints_silver") \
        .trigger(once=True) \
        .toTable(table_name)

    query.awaitTermination()


if __name__ == "__main__":
    save_to_silver()

