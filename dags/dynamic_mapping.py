from airflow.decorators import dag, task
from datetime import datetime
import socket
import time

@dag(
    dag_id="dynamic_task_mapping",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "distributed"],
)
def dynamic_task_mapping():

    @task
    def get_files():
        return [
            "file_1.csv",
            "file_2.csv",
            "file_3.csv",
            "file_4.csv",
            "file_5.csv",
            "file_6.csv",
            "file_7.csv",
            "file_8.csv",
            "file_9.csv",
            "file_10.csv",
        ]

    @task
    def process_file(filename: str):
        worker = socket.gethostname()

        print(f"START {filename} -> {worker}")

        time.sleep(10)

        print(f"END {filename} -> {worker}")

        return {
            "file": filename,
            "worker": worker
        }

    @task
    def summary(results: list):
        print("=== THỐNG KÊ PHÂN PHỐI TASK ===")

        for r in results:
            print(
                f"{r['file']} -> {r['worker']}"
            )

    files = get_files()
    results = process_file.expand(filename=files)
    summary(results)

dynamic_task_mapping()