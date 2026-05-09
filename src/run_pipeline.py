"""Master pipeline script: run all stages in sequence and report timing."""

import subprocess
import sys
import time
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.dirname(os.path.abspath(__file__))

ETAPES = [
    ("Génération des données",  os.path.join(SRC_DIR, "01_data_generator.py")),
    ("Couche Bronze",           os.path.join(SRC_DIR, "02_bronze_layer.py")),
    ("Couche Silver",           os.path.join(SRC_DIR, "03_silver_layer.py")),
    ("Couche Gold",             os.path.join(SRC_DIR, "04_gold_layer.py")),
    ("Visualisations",          os.path.join(SRC_DIR, "05_visualization.py")),
]


def run_pipeline():
    print("=" * 60)
    print("PIPELINE DATA LAKE INDUSTRIEL")
    print("=" * 60)

    timings = []
    total_start = time.time()

    for i, (nom, script) in enumerate(ETAPES, start=1):
        print(f"\n▶ Étape {i}/{len(ETAPES)} : {nom}...")
        start = time.time()
        try:
            result = subprocess.run(
                [sys.executable, script],
                check=True,
                cwd=BASE_DIR,
            )
            elapsed = time.time() - start
            timings.append((nom, elapsed, "✅"))
            print(f"✅ {nom} terminée en {elapsed:.1f}s")
        except subprocess.CalledProcessError as e:
            elapsed = time.time() - start
            timings.append((nom, elapsed, "❌"))
            print(f"❌ ERREUR dans {nom} (code retour {e.returncode})")
            print("   Arrêt du pipeline.")
            break

    total_elapsed = time.time() - total_start

    print("\n" + "=" * 60)
    print(f"{'ÉTAPE':<35} {'DURÉE':>10}  STATUT")
    print("-" * 60)
    for nom, elapsed, status in timings:
        print(f"{nom:<35} {elapsed:>8.1f}s  {status}")
    print("-" * 60)
    print(f"{'TOTAL':<35} {total_elapsed:>8.1f}s")
    print("=" * 60)

    if all(s == "✅" for _, _, s in timings):
        print("\n🎉 Pipeline complet exécuté avec succès !")
        print(f"   Images générées dans : {os.path.join(BASE_DIR, 'data', 'gold', 'visualizations')}")
    else:
        print("\n⚠️  Pipeline terminé avec des erreurs.")
        sys.exit(1)


if __name__ == "__main__":
    run_pipeline()
