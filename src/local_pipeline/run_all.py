import os
import subprocess
import sys
import time
from pathlib import Path


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def run_step(step_name: str, module_name: str, project_root: Path):
    print("=" * 100)
    print(f"Starting step: {step_name}")
    print("=" * 100)

    started_at = time.time()

    env = os.environ.copy()

    # Ensure Spark workers use the same Python executable as the active virtual environment.
    env["PYSPARK_PYTHON"] = sys.executable
    env["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Local Windows Hadoop/winutils setup.
    hadoop_home = r"C:\Users\oglenis\hadoop"
    env["HADOOP_HOME"] = hadoop_home
    env["hadoop.home.dir"] = hadoop_home
    env["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + env.get("PATH", "")

    completed_process = subprocess.run(
        [sys.executable, "-m", module_name],
        cwd=str(project_root),
        env=env,
        text=True,
    )

    duration_seconds = round(time.time() - started_at, 2)

    if completed_process.returncode != 0:
        print("=" * 100)
        print(f"Step failed: {step_name}")
        print(f"Duration: {duration_seconds} seconds")
        print(f"Exit code: {completed_process.returncode}")
        print("=" * 100)
        raise RuntimeError(f"Pipeline stopped because step failed: {step_name}")

    print("=" * 100)
    print(f"Step completed: {step_name}")
    print(f"Duration: {duration_seconds} seconds")
    print("=" * 100)


def main():
    project_root = get_project_root()

    print("Local Credit Risk & Insurance Lakehouse Pipeline")
    print(f"Project root: {project_root}")
    print(f"Python executable: {sys.executable}")

    steps = [
        {
            "step_name": "Bronze ingestion",
            "module_name": "src.local_pipeline.bronze_ingestion",
        },
        {
            "step_name": "Silver transformations",
            "module_name": "src.local_pipeline.silver_transformations",
        },
        {
            "step_name": "Gold feature generation",
            "module_name": "src.local_pipeline.gold_features",
        },
        {
            "step_name": "Data quality and monitoring",
            "module_name": "src.local_pipeline.data_quality_checks",
        },
    ]

    pipeline_started_at = time.time()

    for step in steps:
        run_step(
            step_name=step["step_name"],
            module_name=step["module_name"],
            project_root=project_root,
        )

    total_duration_seconds = round(time.time() - pipeline_started_at, 2)

    print("\n" + "#" * 100)
    print("Pipeline completed successfully.")
    print(f"Total duration: {total_duration_seconds} seconds")
    print("#" * 100)

    print("\nGenerated local lakehouse outputs:")
    print("lakehouse/bronze")
    print("lakehouse/silver")
    print("lakehouse/gold")
    print("lakehouse/monitoring")


if __name__ == "__main__":
    main()