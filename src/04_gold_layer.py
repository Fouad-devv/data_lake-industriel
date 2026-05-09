"""Gold layer: compute KPIs, OEE, MTBF and production summaries."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from setup_spark import get_spark_session, stop_spark
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")
CSV_DIR = os.path.join(GOLD_DIR, "csv")
os.makedirs(CSV_DIR, exist_ok=True)


def save_gold(df, name):
    """Save Gold table as Delta + CSV."""
    delta_path = os.path.join(GOLD_DIR, name)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_path)

    csv_path = os.path.join(CSV_DIR, f"{name}.csv")
    df.toPandas().to_csv(csv_path, index=False)
    print(f"  Sauvegardé : Delta → {delta_path} | CSV → {csv_path}")


# ─── TABLE GOLD 1 : KPIs machines (journalier) ────────────────────────────────

def build_gold_kpis_machines(spark):
    print("\n[1/4] Gold — KPIs Machines (journalier)")

    sensors = spark.read.format("delta").load(os.path.join(SILVER_DIR, "sensors_silver"))
    scada = spark.read.format("delta").load(os.path.join(SILVER_DIR, "scada_silver"))

    # Agréger sensors par jour/machine
    sensors_agg = sensors.groupBy(
        F.to_date("timestamp_clean").alias("date"),
        "machine_id",
    ).agg(
        F.avg("temperature_c").alias("temp_moyenne"),
        F.max("temperature_c").alias("temp_max"),
        F.avg("vibration_mms").alias("vibration_moyenne"),
        F.avg("pression_bar").alias("pression_moyenne"),
        F.sum(F.when(F.col("anomalie_detectee"), 1).otherwise(0)).alias("nb_anomalies_capteurs"),
        F.count("*").alias("nb_mesures"),
    )

    # Agréger SCADA par jour/machine
    scada_agg = scada.groupBy(
        F.to_date("timestamp_clean").alias("date"),
        "machine_id",
    ).agg(
        F.sum(F.when(F.col("etat") == "ALARME", 1).otherwise(0)).alias("nb_alarmes"),
        F.sum(
            F.when(F.col("etat") == "ARRET", F.col("duree_etat_minutes")).otherwise(0)
        ).alias("duree_arret_minutes"),
    )

    df = sensors_agg.join(
        scada_agg,
        on=["date", "machine_id"],
        how="left",
    ).fillna({"nb_alarmes": 0, "duree_arret_minutes": 0})

    df = df.withColumn(
        "taux_disponibilite_pct",
        F.least(
            F.lit(100.0),
            F.greatest(
                F.lit(0.0),
                ((1440.0 - F.col("duree_arret_minutes")) / 1440.0) * 100.0,
            ),
        ),
    )

    save_gold(df, "gold_kpis_machines")
    df.show(10, truncate=False)
    return df


# ─── TABLE GOLD 2 : OEE hebdomadaire ──────────────────────────────────────────

def build_gold_oee(spark):
    print("\n[2/4] Gold — OEE hebdomadaire")

    sensors = spark.read.format("delta").load(os.path.join(SILVER_DIR, "sensors_silver"))
    scada = spark.read.format("delta").load(os.path.join(SILVER_DIR, "scada_silver"))
    mes = spark.read.format("delta").load(os.path.join(SILVER_DIR, "mes_silver"))

    CAPACITE_THEORIQUE_PAR_HEURE = 500.0
    MINUTES_PAR_SEMAINE = 7 * 24 * 60.0

    # Sensors → semaine/machine
    sensors_week = sensors.groupBy(
        F.weekofyear("timestamp_clean").alias("semaine"),
        "machine_id",
    ).agg(
        F.count("*").alias("nb_mesures_semaine"),
    )

    # SCADA → temps d'arrêt par semaine/machine
    scada_week = scada.groupBy(
        F.weekofyear("timestamp_clean").alias("semaine"),
        "machine_id",
    ).agg(
        F.sum(
            F.when(F.col("etat") == "ARRET", F.col("duree_etat_minutes")).otherwise(0)
        ).alias("sum_arret_minutes"),
    )

    # MES → production par semaine/machine
    mes_week = mes.groupBy(
        F.weekofyear("date_debut_clean").alias("semaine"),
        "machine_id",
    ).agg(
        F.sum("quantite_produite").alias("sum_produite"),
        F.sum("quantite_prevue").alias("sum_prevue"),
        F.sum(
            F.col("quantite_produite") * F.col("taux_rebut_pct") / 100.0
        ).alias("sum_rebuts"),
    )

    df = (
        sensors_week.join(scada_week, on=["semaine", "machine_id"], how="left")
        .join(mes_week, on=["semaine", "machine_id"], how="left")
        .fillna({"sum_arret_minutes": 0, "sum_produite": 0, "sum_prevue": 1, "sum_rebuts": 0})
    )

    df = df.withColumn(
        "disponibilite",
        F.least(
            F.lit(1.0),
            F.greatest(
                F.lit(0.0),
                (MINUTES_PAR_SEMAINE - F.col("sum_arret_minutes")) / MINUTES_PAR_SEMAINE,
            ),
        ),
    ).withColumn(
        "temps_disponible_heures",
        (MINUTES_PAR_SEMAINE - F.col("sum_arret_minutes")) / 60.0,
    ).withColumn(
        "performance",
        F.least(
            F.lit(1.0),
            F.greatest(
                F.lit(0.0),
                F.col("sum_produite") / (CAPACITE_THEORIQUE_PAR_HEURE * F.col("temps_disponible_heures")),
            ),
        ),
    ).withColumn(
        "qualite_oee",
        F.least(
            F.lit(1.0),
            F.greatest(
                F.lit(0.0),
                (F.col("sum_produite") - F.col("sum_rebuts")) / F.col("sum_produite"),
            ),
        ),
    ).withColumn(
        "oee_pct",
        F.least(
            F.lit(100.0),
            F.greatest(
                F.lit(0.0),
                F.col("disponibilite") * F.col("performance") * F.col("qualite_oee") * 100.0,
            ),
        ),
    )

    # Round percentages
    for col_name in ["disponibilite", "performance", "qualite_oee"]:
        df = df.withColumn(col_name, F.round(F.col(col_name) * 100, 2))
    df = df.withColumn("oee_pct", F.round("oee_pct", 2))

    save_gold(df, "gold_oee")
    df.show(10, truncate=False)
    return df


# ─── TABLE GOLD 3 : Coûts maintenance ─────────────────────────────────────────

def build_gold_maintenance_cost(spark):
    print("\n[3/4] Gold — Coûts Maintenance & MTBF")

    maintenance = spark.read.format("delta").load(os.path.join(SILVER_DIR, "maintenance_silver"))

    df = maintenance.groupBy("machine_id").agg(
        F.sum(F.when(F.col("type_intervention") == "PREVENTIVE", 1).otherwise(0)).alias(
            "nb_interventions_preventives"
        ),
        F.sum(F.when(F.col("type_intervention") == "CORRECTIVE", 1).otherwise(0)).alias(
            "nb_interventions_correctives"
        ),
        F.sum("cout_eur").alias("cout_total_eur"),
        F.sum("duree_heures").alias("duree_totale_heures"),
        F.avg(
            F.when(F.col("type_intervention") == "CORRECTIVE", F.col("duree_heures"))
        ).alias("mttr_heures"),
    )

    df = df.withColumn(
        "mtbf_heures",
        F.when(
            F.col("nb_interventions_correctives") > 0,
            (30.0 * 24.0) / F.col("nb_interventions_correctives"),
        ).otherwise(F.lit(None).cast("double")),
    )

    df = df.withColumn("cout_total_eur", F.round("cout_total_eur", 2))
    df = df.withColumn("mttr_heures", F.round("mttr_heures", 2))
    df = df.withColumn("mtbf_heures", F.round("mtbf_heures", 2))

    save_gold(df, "gold_maintenance_cost")
    df.show(10, truncate=False)
    return df


# ─── TABLE GOLD 4 : Résumé production ─────────────────────────────────────────

def build_gold_production_summary(spark):
    print("\n[4/4] Gold — Résumé Production")

    mes = spark.read.format("delta").load(os.path.join(SILVER_DIR, "mes_silver"))

    df = mes.groupBy(
        F.month("date_debut_clean").alias("mois"),
        "produit_id",
    ).agg(
        F.sum("quantite_produite").alias("quantite_totale_produite"),
        F.sum("quantite_prevue").alias("quantite_totale_prevue"),
        F.avg("taux_rebut_pct").alias("taux_rebut_moyen_pct"),
        F.sum(F.when(F.col("ordre_statut") == "EN_RETARD", 1).otherwise(0)).alias(
            "nb_ordres_en_retard"
        ),
    ).withColumn(
        "taux_realisation_pct",
        F.round(
            (F.col("quantite_totale_produite") / F.col("quantite_totale_prevue")) * 100.0,
            2,
        ),
    )

    df = df.withColumn("taux_rebut_moyen_pct", F.round("taux_rebut_moyen_pct", 2))

    save_gold(df, "gold_production_summary")
    df.show(10, truncate=False)
    return df


if __name__ == "__main__":
    print("=" * 60)
    print("COUCHE GOLD — KPIs ANALYTIQUES")
    print("=" * 60)

    spark = get_spark_session("GoldLayer")
    try:
        build_gold_kpis_machines(spark)
        build_gold_oee(spark)
        build_gold_maintenance_cost(spark)
        build_gold_production_summary(spark)
        print("\n" + "=" * 60)
        print("✅ Couche Gold terminée.")
        print("=" * 60)
    finally:
        stop_spark(spark)
