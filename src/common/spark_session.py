import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def configure_windows_hadoop():
    hadoop_home = r"C:\Users\oglenis\hadoop"

    if os.name == "nt":
        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["hadoop.home.dir"] = hadoop_home
        os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ.get("PATH", "")


def configure_pyspark_python():
    python_executable = sys.executable

    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable


def get_spark(app_name: str = "credit-risk-insurance-local-lakehouse") -> SparkSession:
    configure_windows_hadoop()
    configure_pyspark_python()

    python_executable = sys.executable

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.pyspark.python", python_executable)
        .config("spark.pyspark.driver.python", python_executable)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark