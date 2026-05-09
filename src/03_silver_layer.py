"""Silver layer: clean, deduplicate, enrich data from Bronze Delta tables."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from setup_spark import get_spark_session, stop_spark
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
os.makedirs(SILVER_DIR, exist_ok=True)


# ─── UDF : parse timestamps with two possible formats ─────────────────────────

def _make_ts_udf():
    from pyspark.sql.functions import udf
    from pyspark.sql.types import TimestampType
    import datetime

    @udf(TimestampType())
    def parse_timestamp(ts_str):
        if ts_str is None:
            return None
        for fmt in (
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%d/%m/%Y %H:%M",
            "%d/%m/%Y %H:%M:%S",
        ):
            try:
                return datetime.datetime.strptime(ts_str, fmt)
            except ValueError:
                continue
        return None

    return parse_timestamp


# ─── SILVER SENSORS ────────────────────────────────────────────────────────────

def process_silver_sensors(spark):
    print("\n[1/4] Traitement Silver — Sensors")
    df = spark.read.format("delta").load(os.path.join(BRONZE_DIR, "sensors"))
    n_before = df.count()

    # 1. Déduplier
    df = df.dropDuplicates(["machine_id", "timestamp"])
    n_after_dedup = df.count()

    # 2. Normaliser timestamp
    parse_ts = _make_ts_udf()
    df = df.withColumn("timestamp_clean", parse_ts(F.col("timestamp").cast("string")))

    # 3. Détecter anomalies
    df = (
        df.withColumn("is_temp_invalid",
                      (F.col("temperature_c") < -10) | (F.col("temperature_c") > 120))
        .withColumn("is_vibration_invalid", F.col("vibration_mms") < 0)
        .withColumn("is_pression_invalid", F.col("pression_bar") < 0)
        .withColumn(
            "anomalie_detectee",
            F.col("is_temp_invalid") | F.col("is_vibration_invalid") | F.col("is_pression_invalid"),
        )
    )

    # 4. Forward-fill NULLs par machine (Window)
    w = (
        Window.partitionBy("machine_id")
        .orderBy("timestamp_clean")
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    for col_name, filled_name in [
        ("temperature_c", "temperature_filled"),
        ("vibration_mms", "vibration_filled"),
        ("pression_bar", "pression_filled"),
        ("courant_a", "courant_filled"),
    ]:
        df = df.withColumn(
            filled_name,
            F.last(F.col(col_name), ignorenulls=True).over(w),
        )
        # Track which values were originally NULL for quality label
        df = df.withColumn(
            f"{col_name}_was_null",
            F.col(col_name).isNull() & F.col(filled_name).isNotNull(),
        )

    was_null_cols = [
        "temperature_c_was_null",
        "vibration_mms_was_null",
        "pression_bar_was_null",
        "courant_a_was_null",
    ]
    df = df.withColumn("any_was_null", F.greatest(*[F.col(c).cast("int") for c in was_null_cols]) == 1)

    # 5. Qualité
    df = df.withColumn(
        "qualite_donnee",
        F.when(F.col("anomalie_detectee"), F.lit("INVALID"))
        .when(F.col("any_was_null"), F.lit("INTERPOLATED"))
        .otherwise(F.lit("VALID")),
    )

    out_path = os.path.join(SILVER_DIR, "sensors_silver")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out_path)

    n_final = df.count()
    n_anomalies = df.filter(F.col("anomalie_detectee")).count()
    print(f"  Avant dédup : {n_before:,} | Après : {n_after_dedup:,} | Dédupliquées : {n_before - n_after_dedup:,}")
    print(f"  Anomalies détectées : {n_anomalies:,} ({100 * n_anomalies / n_final:.1f}%)")
    df.groupBy("qualite_donnee").count().orderBy("qualite_donnee").show()
    return df


# ─── SILVER SCADA ──────────────────────────────────────────────────────────────

def process_silver_scada(spark):
    print("\n[2/4] Traitement Silver — SCADA")
    df = spark.read.format("delta").load(os.path.join(BRONZE_DIR, "scada"))
    n_before = df.count()

    # Normaliser code_alarme
    df = df.withColumn("code_alarme", F.upper(F.col("code_alarme")))

    # Parser timestamp
    parse_ts = _make_ts_udf()
    df = df.withColumn("timestamp_clean", parse_ts(F.col("timestamp").cast("string")))

    # Dédupliquer sur (machine_id, timestamp, etat)
    df = df.dropDuplicates(["machine_id", "timestamp", "etat"])
    n_after = df.count()

    # Durée de l'état : lead sur timestamp suivant
    w_scada = Window.partitionBy("machine_id").orderBy("timestamp_clean")
    df = df.withColumn(
        "next_timestamp",
        F.lead("timestamp_clean").over(w_scada),
    ).withColumn(
        "duree_etat_minutes",
        (F.unix_timestamp("next_timestamp") - F.unix_timestamp("timestamp_clean")) / 60.0,
    )

    out_path = os.path.join(SILVER_DIR, "scada_silver")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out_path)

    print(f"  Avant dédup : {n_before:,} | Après : {n_after:,}")
    df.groupBy("etat").count().orderBy("etat").show()
    return df


# ─── SILVER MES ────────────────────────────────────────────────────────────────

def process_silver_mes(spark):
    print("\n[3/4] Traitement Silver — MES")
    df = spark.read.format("delta").load(os.path.join(BRONZE_DIR, "mes"))

    # Parser timestamps (already ISO strings in CSV)
    parse_ts = _make_ts_udf()
    df = (
        df.withColumn("date_debut_clean", parse_ts(F.col("date_debut").cast("string")))
        .withColumn("date_fin_prevue_clean", parse_ts(F.col("date_fin_prevue").cast("string")))
        .withColumn("date_fin_reelle_clean", parse_ts(F.col("date_fin_reelle").cast("string")))
    )

    # Calculs
    df = (
        df.withColumn(
            "retard_heures",
            (F.unix_timestamp("date_fin_reelle_clean") - F.unix_timestamp("date_fin_prevue_clean")) / 3600.0,
        )
        .withColumn(
            "taux_rendement_pct",
            (F.col("quantite_produite") / F.col("quantite_prevue")) * 100.0,
        )
        .withColumn(
            "ordre_statut",
            F.when(F.col("date_fin_reelle_clean").isNull(), F.lit("EN_COURS"))
            .when(F.col("retard_heures") > 0, F.lit("EN_RETARD"))
            .otherwise(F.lit("A_TEMPS")),
        )
    )

    out_path = os.path.join(SILVER_DIR, "mes_silver")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out_path)

    print(f"  Ordres : {df.count():,}")
    df.groupBy("ordre_statut").count().show()
    return df


# ─── SILVER MAINTENANCE ────────────────────────────────────────────────────────

def process_silver_maintenance(spark):
    print("\n[4/4] Traitement Silver — Maintenance")
    df = spark.read.format("delta").load(os.path.join(BRONZE_DIR, "maintenance"))
    out_path = os.path.join(SILVER_DIR, "maintenance_silver")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out_path)
    print(f"  Interventions : {df.count():,}")
    return df


# ─── JOINTURE ENRICHIE ─────────────────────────────────────────────────────────

def build_sensors_enriched(spark, sensors_df, scada_df):
    print("\n[+] Création sensors_enriched (join sensors + scada)")

    sensors_alias = sensors_df.select(
        "machine_id",
        "timestamp_clean",
        "temperature_filled",
        "vibration_filled",
        "pression_filled",
        "courant_filled",
        "anomalie_detectee",
        "qualite_donnee",
    ).alias("s")

    scada_alias = scada_df.select(
        F.col("machine_id").alias("scada_machine_id"),
        F.col("timestamp_clean").alias("scada_ts"),
        "etat",
        "code_alarme",
        "duree_etat_minutes",
    ).alias("sc")

    joined = sensors_alias.join(
        scada_alias,
        on=(
            (F.col("s.machine_id") == F.col("sc.scada_machine_id"))
            & (
                F.abs(
                    F.unix_timestamp(F.col("s.timestamp_clean"))
                    - F.unix_timestamp(F.col("sc.scada_ts"))
                )
                <= 300
            )
        ),
        how="left",
    ).drop("scada_machine_id")

    out_path = os.path.join(SILVER_DIR, "sensors_enriched")
    joined.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out_path)
    print(f"  Lignes enrichies : {joined.count():,}")
    return joined


if __name__ == "__main__":
    print("=" * 60)
    print("COUCHE SILVER — NETTOYAGE ET ENRICHISSEMENT")
    print("=" * 60)

    spark = get_spark_session("SilverLayer")
    try:
        sensors_df = process_silver_sensors(spark)
        scada_df = process_silver_scada(spark)
        process_silver_mes(spark)
        process_silver_maintenance(spark)
        build_sensors_enriched(spark, sensors_df, scada_df)
        print("\n" + "=" * 60)
        print("✅ Couche Silver terminée.")
        print("=" * 60)
    finally:
        stop_spark(spark)
