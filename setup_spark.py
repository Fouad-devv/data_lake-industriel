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


def _is_valid_java_home(path):
    """Return True if path is a real JDK/JRE with a working java binary."""
    if not path or not os.path.isdir(path):
        return False
    java_bin = os.path.join(path, "bin", "java.exe")
    if os.path.isfile(java_bin):
        return True
    # Linux/Mac
    java_bin_unix = os.path.join(path, "bin", "java")
    return os.path.isfile(java_bin_unix)


def _find_java():
    """Detect Java installation and set JAVA_HOME to a real JDK directory."""
    # Already set and valid → nothing to do
    current = os.environ.get("JAVA_HOME", "")
    if _is_valid_java_home(current):
        return

    # Force the known JDK path directly (discovered from registry scan)
    hardcoded = r"C:\Program Files\Java\jdk-21.0.10"
    if _is_valid_java_home(hardcoded):
        os.environ["JAVA_HOME"] = hardcoded
        return

    # Try to derive from `java` in PATH, but validate the result
    java_exe = shutil.which("java")
    if java_exe:
        java_exe = os.path.abspath(java_exe)
        # Resolve symlinks/junctions so we get the real binary
        try:
            java_exe = os.path.realpath(java_exe)
        except Exception:
            pass
        java_bin_dir = os.path.dirname(java_exe)
        java_home_candidate = os.path.dirname(java_bin_dir)
        if _is_valid_java_home(java_home_candidate):
            os.environ["JAVA_HOME"] = java_home_candidate
            return

    # Scan common install locations
    candidates = [
        r"C:\Program Files\Java\jdk-21.0.10",
        r"C:\Program Files\Java\jdk-21",
        r"C:\Program Files\Java\jdk-17",
        r"C:\Program Files\Java\jdk-11",
        r"C:\Program Files\Eclipse Adoptium\jdk-21.0.3.9-hotspot",
        r"C:\Program Files\Eclipse Adoptium\jdk-17.0.7.7-hotspot",
        r"C:\Program Files\Eclipse Adoptium\jdk-11.0.19.7-hotspot",
        r"C:\Program Files\Microsoft\jdk-17.0.7.7-hotspot",
        r"C:\Program Files\Microsoft\jdk-11.0.19.7-hotspot",
        "/usr/lib/jvm/java-11-openjdk-amd64",
        "/usr/lib/jvm/java-17-openjdk-amd64",
        "/usr/lib/jvm/java-21-openjdk-amd64",
    ]
    for c in candidates:
        if _is_valid_java_home(c):
            os.environ["JAVA_HOME"] = c
            return


# ── Set Python paths at MODULE LOAD TIME so they are ready before pyspark imports ──
_python_exe = _find_python_executable()
os.environ["PYSPARK_PYTHON"] = _python_exe
os.environ["PYSPARK_DRIVER_PYTHON"] = _python_exe

# ── HADOOP_HOME for Windows (winutils.exe + hadoop.dll required by PySpark on Windows) ──
if os.name == "nt":
    _hadoop_home = r"C:\hadoop"
    if os.path.isfile(os.path.join(_hadoop_home, "bin", "winutils.exe")):
        os.environ["HADOOP_HOME"] = _hadoop_home
        os.environ["hadoop.home.dir"] = _hadoop_home
        # Add C:\hadoop\bin to PATH so the JVM can load hadoop.dll via System.loadLibrary("hadoop")
        _hadoop_bin = os.path.join(_hadoop_home, "bin")
        if _hadoop_bin.lower() not in os.environ.get("PATH", "").lower():
            os.environ["PATH"] = _hadoop_bin + os.pathsep + os.environ.get("PATH", "")


_DELTA_JARS_DIR = r"C:\hadoop\delta_jars"
_DELTA_JARS = [
    "delta-spark_2.12-3.0.0.jar",
    "delta-storage-3.0.0.jar",
    "antlr4-runtime-4.9.3.jar",
]


def _get_delta_jars_path():
    """Return comma-separated absolute paths to the Delta Lake JARs."""
    paths = []
    for jar in _DELTA_JARS:
        full = os.path.join(_DELTA_JARS_DIR, jar)
        if os.path.isfile(full):
            paths.append(full)
    return ",".join(paths)


def get_spark_session(app_name="DataLakeIndustriel"):
    """Return a configured SparkSession with Delta Lake support (local JARs, no Maven download)."""
    _find_java()

    delta_jars = _get_delta_jars_path()

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars", delta_jars)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        # Use RawLocalFileSystem instead of ChecksumFileSystem to avoid
        # NativeIO$Windows.access0 UnsatisfiedLinkError on Windows
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark


def stop_spark(spark):
    """Cleanly stop a SparkSession."""
    if spark:
        spark.stop()
