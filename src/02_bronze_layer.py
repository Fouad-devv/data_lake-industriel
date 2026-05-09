"""Bronze layer: ingest raw data into Delta Lake with metadata columns."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from setup_spark import get_spark_session, stop_spark
from pyspark.sql import functions as F

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")


def add_metadata(df, source_file, source_system):
    """Add Bronze metadata columns to a DataFrame."""
    return (
        df.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_row_id", F.monotonically_increasing_id())
    )


def save_bronze(df, path, partition_col="_source_system"):
    """Write DataFrame as Delta Lake table."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy(partition_col)
        .save(path)
    )


def ingest_sensors(spark):
    print("\n[1/4] Ingestion sensors_data.csv → Bronze")
    path = os.path.join(RAW_DIR, "sensors_data.csv")
    df = spark.read.csv(path, header=True, inferSchema=True)
    df = add_metadata(df, "sensors_data.csv", "IOT")
    out_path = os.path.join(BRONZE_DIR, "sensors")
    save_bronze(df, out_path)
    print(f"  Lignes : {df.count():,}")
    df.printSchema()
    df.show(5, truncate=False)
    return df


def ingest_scada(spark):
    print("\n[2/4] Ingestion scada_logs.json → Bronze")
    path = os.path.join(RAW_DIR, "scada_logs.json")
    df = spark.read.json(path, multiLine=True)
    df = add_metadata(df, "scada_logs.json", "SCADA")
    out_path = os.path.join(BRONZE_DIR, "scada")
    save_bronze(df, out_path)
    print(f"  Lignes : {df.count():,}")
    df.printSchema()
    df.show(5, truncate=False)
    return df


def ingest_mes(spark):
    print("\n[3/4] Ingestion mes_orders.csv → Bronze")
    path = os.path.join(RAW_DIR, "mes_orders.csv")
    df = spark.read.csv(path, header=True, inferSchema=True)
    df = add_metadata(df, "mes_orders.csv", "MES")
    out_path = os.path.join(BRONZE_DIR, "mes")
    save_bronze(df, out_path)
    print(f"  Lignes : {df.count():,}")
    df.printSchema()
    df.show(5, truncate=False)
    return df


def ingest_maintenance(spark):
    print("\n[4/4] Ingestion maintenance_logs.csv → Bronze")
    path = os.path.join(RAW_DIR, "maintenance_logs.csv")
    df = spark.read.csv(path, header=True, inferSchema=True)
    df = add_metadata(df, "maintenance_logs.csv", "MAINTENANCE")
    out_path = os.path.join(BRONZE_DIR, "maintenance")
    save_bronze(df, out_path)
    print(f"  Lignes : {df.count():,}")
    df.printSchema()
    df.show(5, truncate=False)
    return df


if __name__ == "__main__":
    print("=" * 60)
    print("COUCHE BRONZE — INGESTION DELTA LAKE")
    print("=" * 60)

    spark = get_spark_session("BronzeLayer")
    try:
        ingest_sensors(spark)
        ingest_scada(spark)
        ingest_mes(spark)
        ingest_maintenance(spark)
        print("\n" + "=" * 60)
        print("✅ Couche Bronze terminée.")
        print("=" * 60)
    finally:
        stop_spark(spark)
