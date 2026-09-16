"""
DAG для автоматизации пайплайна обработки данных Яндекс Метрики.
Запускает 8 этапов обработки ежедневно в 6:00 утра.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Добавляем путь к папке проекта, где лежит pipeline.py
PROJECT_PATH = "/Users/mavrinairina/Desktop/посещаемость сайта"
sys.path.append(PROJECT_PATH)

# Импортируем функции из pipeline.py
from pipeline import (
    check_file_exists,
    validate_columns,
    validate_data_quality,
    preprocess_data,
    check_processed_file,
    init_db,
    load_data_to_db,
    check_table_data,
    RAW_FILE,
    PROCESSED_FILE,
    DB_PATH,
)

# Аргументы по умолчанию для DAG
default_args = {
    "owner": "irina",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Определяем DAG
with DAG(
    dag_id="metrika_pipeline",
    default_args=default_args,
    description="Автоматизация обработки данных Яндекс Метрики",
    schedule_interval="0 6 * * *",   # каждый день в 6:00 утра
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["metrika", "analytics"],
) as dag:

    # Задача 1: проверка наличия файла
    task_01_check_file = PythonOperator(
        task_id="check_file_exists",
        python_callable=check_file_exists,
        op_args=[RAW_FILE],
    )

    # Задача 2: валидация колонок
    task_02_validate_columns = PythonOperator(
        task_id="validate_columns",
        python_callable=validate_columns,
        op_args=[RAW_FILE],
    )

    # Задача 3: проверка качества данных
    task_03_validate_quality = PythonOperator(
        task_id="validate_data_quality",
        python_callable=validate_data_quality,
        op_args=[RAW_FILE],
    )

    # Задача 4: обработка данных
    task_04_preprocess = PythonOperator(
        task_id="preprocess_data",
        python_callable=preprocess_data,
        op_args=[RAW_FILE, PROCESSED_FILE],
    )

    # Задача 5: проверка обработанного файла
    task_05_check_processed = PythonOperator(
        task_id="check_processed_file",
        python_callable=check_processed_file,
        op_args=[PROCESSED_FILE],
    )

    # Задача 6: инициализация БД
    task_06_init_db = PythonOperator(
        task_id="init_db",
        python_callable=init_db,
        op_args=[DB_PATH],
    )

    # Задача 7: загрузка данных в БД
    task_07_load_db = PythonOperator(
        task_id="load_data_to_db",
        python_callable=load_data_to_db,
        op_args=[DB_PATH, PROCESSED_FILE],
    )

    # Задача 8: проверка данных в БД
    task_08_check_table = PythonOperator(
        task_id="check_table_data",
        python_callable=check_table_data,
        op_args=[DB_PATH],
    )

    # Определяем порядок выполнения
    (
        task_01_check_file
        >> task_02_validate_columns
        >> task_03_validate_quality
        >> task_04_preprocess
        >> task_05_check_processed
        >> task_06_init_db
        >> task_07_load_db
        >> task_08_check_table
    )
