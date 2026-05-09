"""Visualization layer: read Gold CSVs and produce analytical dashboards."""

import os
import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(BASE_DIR, "data", "gold", "csv")
VIZ_DIR = os.path.join(BASE_DIR, "data", "gold", "visualizations")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
os.makedirs(VIZ_DIR, exist_ok=True)

PALETTE = ["#E74C3C", "#3498DB", "#2ECC71", "#F39C12", "#9B59B6"]

try:
    plt.style.use("seaborn-v0_8-darkgrid")
except OSError:
    plt.style.use("seaborn-darkgrid")


def load_csv(name):
    path = os.path.join(CSV_DIR, f"{name}.csv")
    return pd.read_csv(path)


# ─── FIGURE 1 : Sensors Dashboard ─────────────────────────────────────────────

def figure_sensors():
    print("  Figure 1 : Sensors Dashboard ...")
    df = load_csv("gold_kpis_machines")
    df["date"] = pd.to_datetime(df["date"])

    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle("Tableau de Bord Capteurs IoT — 30 Jours de Production", fontsize=14, fontweight="bold")

    machines = df["machine_id"].unique()
    machine_colors = {m: PALETTE[i % len(PALETTE)] for i, m in enumerate(sorted(machines))}

    # [0,0] Température moyenne/jour par machine
    ax = axes[0, 0]
    for machine in sorted(machines):
        sub = df[df["machine_id"] == machine].sort_values("date")
        ax.plot(sub["date"], sub["temp_moyenne"], label=machine, color=machine_colors[machine], marker=".")
        # Marquer le max
        idx_max = sub["temp_max"].idxmax()
        ax.scatter(sub.loc[idx_max, "date"], sub.loc[idx_max, "temp_max"],
                   color=machine_colors[machine], marker="^", zorder=5, s=80)
    ax.set_title("Température Moyenne/Jour par Machine")
    ax.set_xlabel("Date")
    ax.set_ylabel("Température (°C)")
    ax.legend(fontsize=8)

    # [0,1] Vibration moyenne/jour par machine
    ax = axes[0, 1]
    for machine in sorted(machines):
        sub = df[df["machine_id"] == machine].sort_values("date")
        ax.plot(sub["date"], sub["vibration_moyenne"], label=machine, color=machine_colors[machine], marker=".")
    ax.set_title("Vibration Moyenne/Jour par Machine")
    ax.set_xlabel("Date")
    ax.set_ylabel("Vibration (mm/s)")
    ax.legend(fontsize=8)

    # [1,0] Alarmes quotidiennes par machine (barplot empilé)
    ax = axes[1, 0]
    if "nb_alarmes" in df.columns:
        pivot = df.pivot_table(index="date", columns="machine_id", values="nb_alarmes", aggfunc="sum", fill_value=0)
        pivot.plot(kind="bar", stacked=True, ax=ax,
                   color=[machine_colors.get(c, "#888") for c in pivot.columns], legend=True)
    ax.set_title("Alarmes Quotidiennes par Machine")
    ax.set_xlabel("Date")
    ax.set_ylabel("Nombre d'alarmes")
    ax.set_xticklabels([])

    # [1,1] Taux de disponibilité par machine (area plot)
    ax = axes[1, 1]
    for machine in sorted(machines):
        sub = df[df["machine_id"] == machine].sort_values("date")
        ax.fill_between(sub["date"], sub["taux_disponibilite_pct"],
                        alpha=0.4, color=machine_colors[machine], label=machine)
        ax.plot(sub["date"], sub["taux_disponibilite_pct"], color=machine_colors[machine])
    ax.set_title("Taux de Disponibilité par Machine (%)")
    ax.set_xlabel("Date")
    ax.set_ylabel("Disponibilité (%)")
    ax.legend(fontsize=8)
    ax.set_ylim(0, 110)

    plt.tight_layout()
    out = os.path.join(VIZ_DIR, "sensors_dashboard.png")
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"    Sauvegardé : {out}")


# ─── FIGURE 2 : OEE Dashboard ─────────────────────────────────────────────────

def figure_oee():
    print("  Figure 2 : OEE Dashboard ...")
    df = load_csv("gold_oee")

    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle("OEE Dashboard — Overall Equipment Effectiveness", fontsize=14, fontweight="bold")

    # [0] Heatmap OEE machine × semaine
    ax = axes[0]
    if "semaine" in df.columns and "machine_id" in df.columns and "oee_pct" in df.columns:
        pivot = df.pivot_table(index="machine_id", columns="semaine", values="oee_pct", aggfunc="mean")
        sns.heatmap(pivot, cmap="RdYlGn", vmin=0, vmax=100, annot=True, fmt=".1f", ax=ax,
                    linewidths=0.5, cbar_kws={"shrink": 0.8})
    ax.set_title("OEE (%) machine × semaine")

    # [1] Barres groupées Disponibilité / Performance / Qualité par machine
    ax = axes[1]
    cols_to_plot = [c for c in ["disponibilite", "performance", "qualite_oee"] if c in df.columns]
    if cols_to_plot:
        agg = df.groupby("machine_id")[cols_to_plot].mean().reset_index()
        x = np.arange(len(agg))
        width = 0.25
        for j, col in enumerate(cols_to_plot):
            ax.bar(x + j * width, agg[col], width, label=col.replace("_", " ").title(), color=PALETTE[j])
        ax.set_xticks(x + width)
        ax.set_xticklabels(agg["machine_id"])
        ax.axhline(85, color="red", linestyle="--", linewidth=1.5, label="Objectif OEE 85%")
        ax.set_ylim(0, 115)
        ax.set_title("Disponibilité / Performance / Qualité par Machine")
        ax.set_ylabel("%")
        ax.legend(fontsize=8)

    # [2] Évolution OEE hebdomadaire
    ax = axes[2]
    if "semaine" in df.columns:
        for i, machine in enumerate(sorted(df["machine_id"].unique())):
            sub = df[df["machine_id"] == machine].sort_values("semaine")
            ax.plot(sub["semaine"], sub["oee_pct"], marker="o",
                    color=PALETTE[i % len(PALETTE)], label=machine)
        ax.axhline(85, color="red", linestyle="--", linewidth=1.5, label="Objectif 85%")
        ax.set_xlabel("Semaine")
        ax.set_ylabel("OEE (%)")
        ax.set_title("Évolution OEE Hebdomadaire")
        ax.legend(fontsize=8)

    plt.tight_layout()
    out = os.path.join(VIZ_DIR, "oee_dashboard.png")
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"    Sauvegardé : {out}")


# ─── FIGURE 3 : Anomalies Dashboard ───────────────────────────────────────────

def figure_anomalies():
    print("  Figure 3 : Anomalies Dashboard ...")

    # Try reading from Silver CSV cache; fall back to Bronze raw CSV
    sensors_silver_csv = os.path.join(CSV_DIR, "sensors_silver_sample.csv")
    raw_csv = os.path.join(BASE_DIR, "data", "raw", "sensors_data.csv")

    if not os.path.exists(sensors_silver_csv):
        # Create a sample from raw for visualization purposes
        df_raw = pd.read_csv(raw_csv)
        df_raw["temperature_c"] = pd.to_numeric(df_raw["temperature_c"], errors="coerce")
        df_raw["vibration_mms"] = pd.to_numeric(df_raw["vibration_mms"], errors="coerce")
        df_raw["anomalie_detectee"] = (
            (df_raw["temperature_c"] < -10) | (df_raw["temperature_c"] > 120) |
            (df_raw["vibration_mms"] < 0)
        )
        df = df_raw.dropna(subset=["temperature_c", "vibration_mms"]).sample(
            min(5000, len(df_raw)), random_state=42
        )
    else:
        df = pd.read_csv(sensors_silver_csv)

    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle("Analyse des Anomalies Capteurs", fontsize=14, fontweight="bold")

    # [0] Scatter température vs vibration
    ax = axes[0]
    normal = df[df["anomalie_detectee"] == False]
    anomalies = df[df["anomalie_detectee"] == True]
    ax.scatter(normal["temperature_c"], normal["vibration_mms"],
               c="#3498DB", alpha=0.3, s=10, label="Normal")
    ax.scatter(anomalies["temperature_c"], anomalies["vibration_mms"],
               c="#E74C3C", alpha=0.5, s=15, label="Anomalie")
    ax.axvline(95, color="red", linestyle="--", linewidth=1, label="Seuil 95°C")
    ax.add_patch(mpatches.FancyArrowPatch(
        (0, 0), (0, 0), arrowstyle="-", color="none"))
    rect = mpatches.Rectangle((95, ax.get_ylim()[0] if ax.get_ylim()[0] != 0 else 0),
                               40, 10, linewidth=1, edgecolor="red",
                               facecolor="red", alpha=0.1, label="Zone surchauffe")
    ax.add_patch(rect)
    ax.set_xlabel("Température (°C)")
    ax.set_ylabel("Vibration (mm/s)")
    ax.set_title("Température vs Vibration")
    ax.legend(fontsize=8)

    # [1] Violinplot distribution température par machine
    ax = axes[1]
    df_valid = df[df["temperature_c"].between(-10, 130)]
    if len(df_valid) > 0:
        machine_palette = {m: PALETTE[i % len(PALETTE)]
                           for i, m in enumerate(sorted(df_valid["machine_id"].unique()))}
        sns.violinplot(data=df_valid, x="machine_id", y="temperature_c",
                       palette=machine_palette, ax=ax, order=sorted(df_valid["machine_id"].unique()))
    ax.axhline(95, color="red", linestyle="--", linewidth=1.5, label="Seuil surchauffe 95°C")
    ax.set_xlabel("Machine")
    ax.set_ylabel("Température (°C)")
    ax.set_title("Distribution Température par Machine")
    ax.legend(fontsize=8)

    plt.tight_layout()
    out = os.path.join(VIZ_DIR, "anomalies_dashboard.png")
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"    Sauvegardé : {out}")


# ─── FIGURE 4 : Maintenance Dashboard ─────────────────────────────────────────

def figure_maintenance():
    print("  Figure 4 : Maintenance Dashboard ...")
    df = load_csv("gold_maintenance_cost")

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("KPIs Maintenance — Coûts et Fiabilité", fontsize=14, fontweight="bold")

    # [0] Pie chart répartition coût
    ax = axes[0]
    if "nb_interventions_preventives" in df.columns:
        cout_prev = (
            df["cout_total_eur"] * df["nb_interventions_preventives"] /
            (df["nb_interventions_preventives"] + df["nb_interventions_correctives"])
        ).sum()
        cout_corr = df["cout_total_eur"].sum() - cout_prev
        sizes = [cout_prev, cout_corr]
        labels = ["Préventive", "Corrective"]
        colors = ["#2ECC71", "#E74C3C"]
        explode = (0.0, 0.05)
        wedges, texts, autotexts = ax.pie(
            sizes, labels=labels, colors=colors, explode=explode,
            autopct="%1.1f%%", startangle=140, pctdistance=0.75
        )
        cout_total = df["cout_total_eur"].sum()
        ax.text(0, 0, f"{cout_total:,.0f} €", ha="center", va="center",
                fontsize=10, fontweight="bold")
    ax.set_title("Répartition Coût Maintenance")

    # [1] MTBF horizontal bar chart par machine
    ax = axes[1]
    if "mtbf_heures" in df.columns:
        df_sorted = df.dropna(subset=["mtbf_heures"]).sort_values("mtbf_heures")
        colors = ["#2ECC71" if v >= 200 else "#E74C3C" for v in df_sorted["mtbf_heures"]]
        bars = ax.barh(df_sorted["machine_id"], df_sorted["mtbf_heures"], color=colors)
        ax.axvline(200, color="red", linestyle="--", linewidth=1.5, label="Objectif MTBF 200h")
        # Valeurs sur les barres
        for bar, val in zip(bars, df_sorted["mtbf_heures"]):
            ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height() / 2,
                    f"{val:.1f}h", va="center", fontsize=9)
        ax.set_xlabel("MTBF (heures)")
        ax.set_title("MTBF par Machine")
        ax.legend(fontsize=8)

    plt.tight_layout()
    out = os.path.join(VIZ_DIR, "maintenance_dashboard.png")
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"    Sauvegardé : {out}")


# ─── RÉSUMÉ FINAL ──────────────────────────────────────────────────────────────

def print_summary():
    print("\n" + "=" * 60)
    print("RÉSUMÉ DU DATA LAKE INDUSTRIEL")
    print("=" * 60)

    # Bronze
    raw_dir = os.path.join(BASE_DIR, "data", "raw")
    n_sensors = len(pd.read_csv(os.path.join(raw_dir, "sensors_data.csv")))
    n_scada_json = os.path.join(raw_dir, "scada_logs.json")
    import json
    with open(n_scada_json) as f:
        n_scada = len(json.load(f))
    n_mes = len(pd.read_csv(os.path.join(raw_dir, "mes_orders.csv")))
    n_maint = len(pd.read_csv(os.path.join(raw_dir, "maintenance_logs.csv")))
    n_bronze = n_sensors + n_scada + n_mes + n_maint

    # Silver anomalies (approx from raw)
    df_sensors = pd.read_csv(os.path.join(raw_dir, "sensors_data.csv"))
    df_sensors["temperature_c"] = pd.to_numeric(df_sensors["temperature_c"], errors="coerce")
    df_sensors["vibration_mms"] = pd.to_numeric(df_sensors["vibration_mms"], errors="coerce")
    n_anomalies = (
        (df_sensors["temperature_c"] < -10) | (df_sensors["temperature_c"] > 120) |
        (df_sensors["vibration_mms"] < 0)
    ).sum()
    n_silver = n_sensors
    pct_anomalies = 100 * n_anomalies / n_silver if n_silver else 0

    # Gold
    df_oee = load_csv("gold_oee")
    oee_moyen = df_oee["oee_pct"].mean() if "oee_pct" in df_oee.columns else 0

    df_maint = load_csv("gold_maintenance_cost")
    cout_total = df_maint["cout_total_eur"].sum() if "cout_total_eur" in df_maint.columns else 0

    print(f"  Bronze : {n_bronze:,} lignes ingérées depuis 4 sources")
    print(f"  Silver : {n_silver:,} lignes capteurs nettoyées ({pct_anomalies:.1f}% anomalies)")
    print(f"  Gold   : 4 tables analytiques produites")
    print(f"  OEE moyen global : {oee_moyen:.1f}%")
    print(f"  Coût maintenance total : {cout_total:,.0f} €")
    print("=" * 60)


if __name__ == "__main__":
    print("=" * 60)
    print("VISUALISATIONS — DASHBOARDS ANALYTIQUES")
    print("=" * 60)
    figure_sensors()
    figure_oee()
    figure_anomalies()
    figure_maintenance()
    print_summary()
    print("✅ Visualisations terminées.")
