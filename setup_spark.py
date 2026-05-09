import os
import shutil


def _find_java():
    """Detect Java installation and set JAVA_HOME if needed."""
    if os.environ.get("JAVA_HOME"):
        return

    candidates = [
        r"C:\Program Files\Java\jdk-11",
        r"C:\Program Files\Java\jdk-17",
        r"C:\Program Files\Java\jdk-8",
        r"C:\Program Files\Eclipse Adoptium\jdk-11.0.19.7-hotspot",
        r"C:\Program Files\Eclipse Adoptium\jdk-17.0.7.7-hotspot",
        r"C:\Program Files\Microsoft\jdk-11.0.19.7-hotspot",
        r"C:\Program Files\Microsoft\jdk-17.0.7.7-hotspot",
        "/usr/lib/jvm/java-11-openjdk-amd64",
        "/usr/lib/jvm/java-11-openjdk",
        "/usr/lib/jvm/java-8-openjdk-amd64",
    ]

    java_exe = shutil.which("java")
    if java_exe:
        java_bin = os.path.dirname(java_exe)
        java_home = os.path.dirname(java_bin)
        os.environ["JAVA_HOME"] = java_home
        return

    for candidate in candidates:
        if os.path.isdir(candidate):
            os.environ["JAVA_HOME"] = candidate
            return


def get_spark_session(app_name="DataLakeIndustriel"):
    """Return a configured SparkSession with Delta Lake support."""
    _find_java()

    os.environ["PYSPARK_PYTHON"] = shutil.which("python") or "python"

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
