from airflow.decorators import dag, task
from datetime import datetime


@dag(
    dag_id="dynamic_task_mapping",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dynamic_mapping"],
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
        ]

    @task
    def process_file(filename: str):
        print(f"Đang xử lý: {filename}")
        return f"Hoàn thành: {filename}"

    @task
    def summary(results: list):
        print("=== KẾT QUẢ DYNAMIC TASK MAPPING ===")
        for r in results:
            print(r)

    files = get_files()
    results = process_file.expand(filename=files)
    summary(results)


dynamic_task_mapping()