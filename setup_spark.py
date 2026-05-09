import os
import sys
import shutil


def _find_python_executable():
    """Find the real Python executable that PySpark can use (os.path.isfile must return True)."""
    # 1. sys.executable works directly
    if os.path.isfile(sys.executable):
        return sys.executable

    # 2. Look near sys.prefix (Python install root)
    for name in ["python.exe", "python3.exe"]:
        candidate = os.path.join(sys.prefix, name)
        if os.path.isfile(candidate):
            return candidate

    # 3. Derive from where pyspark is installed
    #    pyspark → site-packages → (Lib →) Python root
    try:
        import importlib.util
        spec = importlib.util.find_spec("pyspark")
        if spec and spec.origin:
            pyspark_init = spec.origin
            site_packages = os.path.dirname(os.path.dirname(pyspark_init))
            for go_up in [site_packages, os.path.dirname(site_packages)]:
                for name in ["python.exe", "python3.exe"]:
                    c = os.path.join(go_up, name)
                    if os.path.isfile(c):
                        return c
    except Exception:
        pass

    # 4. Common Windows install paths
    username = os.environ.get("USERNAME", "")
    candidates = [
        rf"C:\Users\{username}\AppData\Local\Programs\Python\Python313\python.exe",
        rf"C:\Users\{username}\AppData\Local\Programs\Python\Python312\python.exe",
        rf"C:\Users\{username}\AppData\Local\Programs\Python\Python311\python.exe",
        r"C:\Program Files\Python313\python.exe",
        r"C:\Python313\python.exe",
        r"C:\Python312\python.exe",
    ]
    for c in candidates:
        if os.path.isfile(c):
            return c

    # 5. Last resort: py launcher
    py = shutil.which("py")
    if py and os.path.isfile(py):
        return py

    return sys.executable  # give up


def _find_java():
    """Detect Java installation and set JAVA_HOME if needed."""
    if os.environ.get("JAVA_HOME") and os.path.isdir(os.environ["JAVA_HOME"]):
        return

    java_exe = shutil.which("java")
    if java_exe:
        java_bin = os.path.dirname(os.path.abspath(java_exe))
        java_home = os.path.dirname(java_bin)
        if os.path.isdir(java_home):
            os.environ["JAVA_HOME"] = java_home
            return

    candidates = [
        r"C:\Program Files\Java\jdk-21.0.10",
        r"C:\Program Files\Java\jdk-21",
        r"C:\Program Files\Java\latest",
        r"C:\Program Files\Java\jdk-17",
        r"C:\Program Files\Java\jdk-11",
        r"C:\Program Files\Java\jdk-8",
        r"C:\Program Files\Eclipse Adoptium\jdk-21.0.3.9-hotspot",
        r"C:\Program Files\Eclipse Adoptium\jdk-17.0.7.7-hotspot",
        r"C:\Program Files\Eclipse Adoptium\jdk-11.0.19.7-hotspot",
        r"C:\Program Files\Microsoft\jdk-17.0.7.7-hotspot",
        r"C:\Program Files\Microsoft\jdk-11.0.19.7-hotspot",
        r"C:\Program Files\OpenJDK\jdk-11",
        r"C:\Program Files\OpenJDK\jdk-17",
        "/usr/lib/jvm/java-11-openjdk-amd64",
        "/usr/lib/jvm/java-17-openjdk-amd64",
    ]
    for c in candidates:
        if os.path.isdir(c):
            os.environ["JAVA_HOME"] = c
            return


# ── Set Python paths at MODULE LOAD TIME so they are ready before pyspark imports ──
_python_exe = _find_python_executable()
os.environ["PYSPARK_PYTHON"] = _python_exe
os.environ["PYSPARK_DRIVER_PYTHON"] = _python_exe


def get_spark_session(app_name="DataLakeIndustriel"):
    """Return a configured SparkSession with Delta Lake support."""
    _find_java()

    # Set SPARK_HOME explicitly from pyspark package location
    try:
        import importlib.util
        spec = importlib.util.find_spec("pyspark")
        if spec and spec.origin:
            spark_home = os.path.dirname(os.path.dirname(spec.origin))
            # spark_home = ...site-packages/pyspark
            spark_home = os.path.dirname(spec.origin)
            os.environ.setdefault("SPARK_HOME", spark_home)
    except Exception:
        pass

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:3.0.0",
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark


def stop_spark(spark):
    """Cleanly stop a SparkSession."""
    if spark:
        spark.stop()
