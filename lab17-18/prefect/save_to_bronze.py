from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from delta import *


def save_to_bronze():
    # Ініціалізація SparkSession
    builder = SparkSession.builder \
        .appName("SaveToBronze") \
        .master("spark://spark:7077") \
        .config("spark.sql.warehouse.dir", "/opt/prefect/spark-warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")

    packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"]
    spark = configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()

    # Визначення схеми JSON
    schema = StructType([
        StructField("id", IntegerType()),
        StructField("sol", IntegerType()),
        StructField("camera", StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("rover_id", IntegerType()),
            StructField("full_name", StringType())
        ])),
        StructField("img_src", StringType()),
        StructField("earth_date", StringType()),
        StructField("rover", StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("landing_date", StringType()),
            StructField("launch_date", StringType()),
            StructField("status", StringType())
        ]))
    ])

    table_name = "delta_bronze"
    DeltaTable.createIfNotExists(spark) \
        .tableName(table_name) \
        .property("delta.enableChangeDataFeed", "true") \
        .addColumns(schema) \
        .execute()

    # Читання потоку з Kafka
    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "curiosity-topic") \
        .option("startingOffsets", "earliest") \
        .load()

    df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select(
            col("data.id").alias("id"),
            col("data.sol").alias("sol"),
            col("data.camera.id").alias("camera_id"),
            col("data.camera.name").alias("camera_name"),
            col("data.camera.full_name").alias("camera_full_name"),
            col("data.img_src").alias("img_src"),
            col("data.earth_date").alias("earth_date"),
            col("data.rover.id").alias("rover_id"),
            col("data.rover.name").alias("rover_name"),
            col("data.rover.landing_date").alias("landing_date"),
            col("data.rover.launch_date").alias("launch_date"),
            col("data.rover.status").alias("status")
        )

    # Запис у Delta Lake
    query = df.writeStream.format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints_bronze") \
        .option("mergeSchema", "true") \
        .trigger(once=True) \
        .toTable(table_name)

    query.awaitTermination()


if __name__ == "__main__":
    save_to_bronze()

