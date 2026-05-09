# Data Lake Industriel — Architecture Bronze-Silver-Gold

Projet académique de Data Engineering industriel avec PySpark, Delta Lake et visualisations avancées.

## Prérequis

- **Python 3.9+**
- **Java 8 ou 11** (requis par PySpark) → [Télécharger OpenJDK 11](https://adoptium.net/temurin/releases/?version=11)
- **VS Code** avec l'extension Python

## Installation

```bash
pip install -r requirements.txt
```

## Lancement

```bash
# Pipeline complet (recommandé)
python src/run_pipeline.py

# Ou étape par étape
python src/01_data_generator.py     # Génération des données brutes
python src/02_bronze_layer.py       # Ingestion PySpark → Delta Lake
python src/03_silver_layer.py       # Nettoyage + enrichissement
python src/04_gold_layer.py         # KPIs, OEE, MTBF
python src/05_visualization.py      # Dashboards matplotlib/seaborn
```

## Structure du projet

```
data_lake_industriel/
├── requirements.txt
├── setup_spark.py              ← Configuration PySpark réutilisable
├── src/
│   ├── 01_data_generator.py    ← 4 sources simulées (43 200 mesures capteurs)
│   ├── 02_bronze_layer.py      ← Ingestion brute + métadonnées Delta
│   ├── 03_silver_layer.py      ← Déduplication, nettoyage, UDFs, Window
│   ├── 04_gold_layer.py        ← KPIs, OEE, MTBF, résumé production
│   ├── 05_visualization.py     ← 4 dashboards PNG 300 DPI
│   └── run_pipeline.py         ← Pipeline maître
└── data/
    ├── raw/                    ← CSV et JSON bruts
    ├── bronze/                 ← Delta Lake partitionné par source
    ├── silver/                 ← Données nettoyées et enrichies
    └── gold/
        ├── csv/                ← Exports CSV des tables analytiques
        └── visualizations/     ← PNG des dashboards
```

## Sources de données simulées

| Source | Format | Lignes | Description |
|--------|--------|--------|-------------|
| `sensors_data.csv` | CSV | ~44 200 | IoT capteurs × 5 machines × 30 jours |
| `scada_logs.json` | JSON | ~2 200 | Événements SCADA (états machines) |
| `mes_orders.csv` | CSV | 200 | Ordres de fabrication MES |
| `maintenance_logs.csv` | CSV | 150 | Interventions de maintenance |

## Couches du Data Lake

| Couche | Technologie | Contenu |
|--------|-------------|---------|
| **Bronze** | Delta Lake | Données brutes + métadonnées d'ingestion |
| **Silver** | Delta Lake | Dédupliquées, timestamps normalisés, anomalies marquées |
| **Gold** | Delta Lake + CSV | KPIs journaliers, OEE hebdomadaire, MTBF, résumé production |

## KPIs calculés

- **OEE** (Overall Equipment Effectiveness) = Disponibilité × Performance × Qualité
- **MTBF** (Mean Time Between Failures) = 30j × 24h / nb_interventions_correctives
- **MTTR** (Mean Time To Repair) = durée moyenne des interventions correctives
- **Taux de disponibilité** = (1440 - minutes_arrêt) / 1440 × 100

## Technologies utilisées

- **PySpark 3.5** — traitement distribué en mode local
- **Delta Lake 3.0** — format de stockage transactionnel (ACID)
- **pandas 2.1** — manipulation légère pour les visualisations
- **matplotlib 3.7 / seaborn 0.12** — dashboards analytiques
- **Faker 19** — génération de données réalistes
