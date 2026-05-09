"""Generate synthetic industrial data for the Data Lake project."""

import io
import json
import os
import random
import sys
from datetime import datetime, timedelta

# Force UTF-8 output on Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import numpy as np
import pandas as pd
from faker import Faker

random.seed(42)
np.random.seed(42)
fake = Faker("fr_FR")
Faker.seed(42)

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")
os.makedirs(BASE_DIR, exist_ok=True)

START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 1, 31, 23, 55)
MACHINES = ["M001", "M002", "M003", "M004", "M005"]


# ─── SOURCE 1 : sensors_data.csv ──────────────────────────────────────────────

def generate_sensors():
    print("  Génération sensors_data.csv ...")
    records = []
    timestamps_5min = pd.date_range(start=START_DATE, end=END_DATE, freq="5min")

    for machine in MACHINES:
        n = len(timestamps_5min)
        temperatures = np.random.normal(65, 10, n).clip(20, 100)
        vibrations = np.random.normal(2.5, 0.8, n).clip(0.1, 8)
        pressions = np.random.normal(4.5, 0.5, n).clip(2, 7)
        courants = np.random.normal(12, 2, n).clip(5, 20)

        for i, ts in enumerate(timestamps_5min):
            # Format mixte : 50% ISO8601, 50% DD/MM/YYYY HH:MM
            if i % 2 == 0:
                ts_str = ts.isoformat()
            else:
                ts_str = ts.strftime("%d/%m/%Y %H:%M")

            records.append({
                "timestamp": ts_str,
                "machine_id": machine,
                "temperature_c": round(temperatures[i], 2),
                "vibration_mms": round(vibrations[i], 3),
                "pression_bar": round(pressions[i], 3),
                "courant_a": round(courants[i], 2),
                "statut_capteur": "OK",
            })

    df = pd.DataFrame(records)
    total = len(df)

    # 3% NULL sur une colonne numérique aléatoire
    num_cols = ["temperature_c", "vibration_mms", "pression_bar", "courant_a"]
    null_idx = df.sample(frac=0.03, random_state=1).index
    for idx in null_idx:
        col = random.choice(num_cols)
        df.at[idx, col] = None

    # 2% duplications
    dup_idx = df.sample(frac=0.02, random_state=2).index
    dups = df.loc[dup_idx].copy()
    df = pd.concat([df, dups], ignore_index=True)

    # 1% température impossible (négatif)
    impossible_neg_idx = df.sample(frac=0.01, random_state=3).index
    df.loc[impossible_neg_idx, "temperature_c"] = [
        round(random.uniform(-20, -5), 2) for _ in impossible_neg_idx
    ]

    # 1% température surchauffe
    overheat_idx = df.sample(frac=0.01, random_state=4).index
    df.loc[overheat_idx, "temperature_c"] = [
        round(random.uniform(100, 130), 2) for _ in overheat_idx
    ]

    # 0.5% vibration impossible (négatif)
    neg_vib_idx = df.sample(frac=0.005, random_state=5).index
    df.loc[neg_vib_idx, "vibration_mms"] = [
        round(random.uniform(-5, -0.1), 3) for _ in neg_vib_idx
    ]

    out_path = os.path.join(BASE_DIR, "sensors_data.csv")
    df.to_csv(out_path, index=False)
    print(f"    → {len(df):,} lignes écrites dans {out_path}")
    return df


# ─── SOURCE 2 : scada_logs.json ───────────────────────────────────────────────

def generate_scada():
    print("  Génération scada_logs.json ...")
    ETATS = ["EN_MARCHE", "ARRET", "ALARME", "MAINTENANCE"]
    WEIGHTS = [0.70, 0.15, 0.10, 0.05]
    ALARME_CODES = [f"ALM_{str(i).zfill(3)}" for i in range(1, 21)]

    events = []
    event_counter = 1

    for _ in range(2000):
        ts = START_DATE + timedelta(seconds=random.randint(0, int((END_DATE - START_DATE).total_seconds())))
        machine = random.choice(MACHINES)
        etat = random.choices(ETATS, weights=WEIGHTS, k=1)[0]
        operateur = f"OP_{str(random.randint(1, 10)).zfill(2)}"

        code_alarme = None
        if etat == "ALARME":
            code_alarme = random.choice(ALARME_CODES)

        descriptions = {
            "EN_MARCHE": fake.sentence(nb_words=6),
            "ARRET": f"Arrêt programmé — {fake.sentence(nb_words=4)}",
            "ALARME": f"Alarme détectée : {fake.sentence(nb_words=5)}",
            "MAINTENANCE": f"Maintenance en cours : {fake.sentence(nb_words=4)}",
        }

        events.append({
            "event_id": f"EVT_{str(event_counter).zfill(5)}",
            "timestamp": ts.isoformat(),
            "machine_id": machine,
            "etat": etat,
            "operateur_id": operateur,
            "code_alarme": code_alarme,
            "description": descriptions[etat],
        })
        event_counter += 1

        # Règle : ALARME → ARRET 30 min après sur la même machine
        if etat == "ALARME":
            ts_arret = ts + timedelta(minutes=30)
            events.append({
                "event_id": f"EVT_{str(event_counter).zfill(5)}",
                "timestamp": ts_arret.isoformat(),
                "machine_id": machine,
                "etat": "ARRET",
                "operateur_id": operateur,
                "code_alarme": None,
                "description": f"Arrêt suite à alarme {code_alarme}",
            })
            event_counter += 1

    out_path = os.path.join(BASE_DIR, "scada_logs.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(events, f, indent=2, ensure_ascii=False)
    print(f"    → {len(events):,} événements écrits dans {out_path}")
    return events


# ─── SOURCE 3 : mes_orders.csv ────────────────────────────────────────────────

def generate_mes():
    print("  Génération mes_orders.csv ...")
    produits = ["PROD_A", "PROD_B", "PROD_C", "PROD_D", "PROD_E"]
    records = []

    for i in range(200):
        machine = random.choice(MACHINES)
        produit = random.choice(produits)
        offset_seconds = random.randint(0, int((END_DATE - START_DATE).total_seconds()))
        date_debut = START_DATE + timedelta(seconds=offset_seconds)
        duree_prevue = timedelta(hours=random.uniform(2, 24))
        date_fin_prevue = date_debut + duree_prevue

        r = random.random()
        if r < 0.70:
            date_fin_reelle = date_fin_prevue + timedelta(minutes=random.uniform(-30, 30))
        elif r < 0.90:
            date_fin_reelle = date_fin_prevue + timedelta(hours=random.uniform(1, 8))
        else:
            date_fin_reelle = None

        quantite_prevue = random.randint(100, 1000)
        quantite_produite = int(min(quantite_prevue, quantite_prevue * np.random.normal(0.92, 0.05)))
        quantite_produite = max(0, quantite_produite)
        taux_rebut = float(np.clip(np.random.normal(2.5, 1.0), 0, 10))

        records.append({
            "ordre_id": f"ORD_{str(i + 1).zfill(5)}",
            "produit_id": produit,
            "machine_id": machine,
            "date_debut": date_debut.isoformat(),
            "date_fin_prevue": date_fin_prevue.isoformat(),
            "date_fin_reelle": date_fin_reelle.isoformat() if date_fin_reelle else None,
            "quantite_prevue": quantite_prevue,
            "quantite_produite": quantite_produite,
            "taux_rebut_pct": round(taux_rebut, 2),
        })

    df = pd.DataFrame(records)
    out_path = os.path.join(BASE_DIR, "mes_orders.csv")
    df.to_csv(out_path, index=False)
    print(f"    → {len(df):,} ordres écrits dans {out_path}")
    return df


# ─── SOURCE 4 : maintenance_logs.csv ──────────────────────────────────────────

def generate_maintenance():
    print("  Génération maintenance_logs.csv ...")
    records = []

    for i in range(150):
        machine = random.choice(MACHINES)
        offset_seconds = random.randint(0, int((END_DATE - START_DATE).total_seconds()))
        date_intervention = START_DATE + timedelta(seconds=offset_seconds)

        type_interv = random.choices(["PREVENTIVE", "CORRECTIVE"], weights=[0.40, 0.60], k=1)[0]

        if type_interv == "PREVENTIVE":
            duree = float(np.clip(np.random.normal(2, 0.5), 0.5, 24))
            cout = float(np.random.normal(500, 100))
            pieces = None
        else:
            duree = float(np.clip(np.random.normal(6, 2), 0.5, 24))
            cout = float(np.random.normal(2000, 500))
            pieces = f"{fake.word()} / {fake.word()}"

        technicien = f"TECH_{str(random.randint(1, 5)).zfill(2)}"

        records.append({
            "intervention_id": f"INT_{str(i + 1).zfill(5)}",
            "date_intervention": date_intervention.isoformat(),
            "machine_id": machine,
            "type_intervention": type_interv,
            "duree_heures": round(duree, 2),
            "technicien_id": technicien,
            "cout_eur": round(abs(cout), 2),
            "description_panne": fake.sentence(nb_words=8),
            "pieces_changees": pieces,
        })

    df = pd.DataFrame(records)
    out_path = os.path.join(BASE_DIR, "maintenance_logs.csv")
    df.to_csv(out_path, index=False)
    print(f"    → {len(df):,} interventions écrites dans {out_path}")
    return df


# ─── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("GÉNÉRATION DES DONNÉES BRUTES")
    print("=" * 60)
    generate_sensors()
    generate_scada()
    generate_mes()
    generate_maintenance()
    print("=" * 60)
    print("✅ Toutes les données brutes ont été générées.")
    print("=" * 60)
