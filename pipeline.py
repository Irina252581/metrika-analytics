import os
import sqlite3
import pandas as pd


RAW_FILE = "Посещаемость-2025-09-01-2026-09-01.csv"
PROCESSED_FILE = "processed_visits.csv"
DB_PATH = "metrika.db"

CSV_PARAMS = {
    "sep": ",",
    "encoding": "utf-8-sig",
}

COLUMN_MAPPING = {
    "Интервал дат визита": "visit_date",
    "Визиты": "visits",
    "Посетители": "visitors",
    "Просмотры": "pageviews",
    "Доля новых посетителей": "new_visitors_share",
    "Отказы": "bounce_rate",
    "Глубина просмотра": "page_depth",
    "Время на сайте": "time_on_site",
}


def check_file_exists(file_path):
    print("[ЭТАП 01] Проверка файла:", file_path)
    if not os.path.isfile(file_path):
        raise FileNotFoundError("Файл не найден: " + file_path)
    print("  OK")
    return True


def validate_columns(file_path):
    print("[ЭТАП 02] Проверка колонок")
    df = pd.read_csv(file_path, nrows=0, **CSV_PARAMS)
    actual = df.columns.tolist()
    print("  Найдены колонки:", actual)
    required = ["Интервал дат визита", "Визиты"]
    missing = [c for c in required if c not in actual]
    if missing:
        raise ValueError("Нет колонок: " + str(missing))
    print("  OK")
    return True


def validate_data_quality(file_path):
    print("[ЭТАП 03] Проверка качества данных")
    df = pd.read_csv(file_path, **CSV_PARAMS)
    df = df[df["Интервал дат визита"] != "Итого и средние"]
    errors = []
    for col in ["Интервал дат визита", "Визиты"]:
        if col in df.columns and df[col].isnull().any():
            errors.append("Пустые значения в колонке: " + col)
    if errors:
        raise ValueError(" | ".join(errors))
    print("  OK, строк:", len(df))
    return True


def preprocess_data(input_file, output_file):
    print("[ЭТАП 04] Обработка данных")
    df = pd.read_csv(input_file, **CSV_PARAMS)
    df = df[df["Интервал дат визита"] != "Итого и средние"]
    df = df.rename(columns=COLUMN_MAPPING)
    df["visit_date"] = pd.to_datetime(df["visit_date"], errors="coerce")
    df = df.dropna(subset=["visit_date"])
    numeric_cols = ["visits", "visitors", "pageviews",
                    "new_visitors_share", "bounce_rate", "page_depth"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "time_on_site" in df.columns:
        def time_to_sec(t):
            if pd.isna(t) or str(t) == "00:00:00":
                return 0
            try:
                h, m, s = map(int, str(t).split(":"))
                return h * 3600 + m * 60 + s
            except Exception:
                return 0
        df["time_on_site_sec"] = df["time_on_site"].apply(time_to_sec)
        df = df.drop(columns=["time_on_site"])
    df.to_csv(output_file, index=False)
    print("  OK, сохранено строк:", len(df))
    return output_file


def check_processed_file(output_file):
    print("[ЭТАП 05] Проверка обработанного файла")
    if not os.path.isfile(output_file):
        raise FileNotFoundError("Файл не создан: " + output_file)
    df = pd.read_csv(output_file)
    if df.empty:
        raise ValueError("Файл пуст")
    if "time_on_site_sec" not in df.columns:
        raise ValueError("Нет колонки time_on_site_sec")
    print("  OK, строк:", len(df))
    return True


def init_db(db_path):
    print("[ЭТАП 06] Инициализация БД")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS visits_data ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "visit_date DATE,"
        "visits INTEGER,"
        "visitors INTEGER,"
        "pageviews INTEGER,"
        "new_visitors_share REAL,"
        "bounce_rate REAL,"
        "page_depth REAL,"
        "time_on_site_sec INTEGER,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    conn.commit()
    conn.close()
    print("  OK")
    return True


def load_data_to_db(db_path, processed_file):
    print("[ЭТАП 07] Загрузка в БД")
    df = pd.read_csv(processed_file)
    conn = sqlite3.connect(db_path)
    df.to_sql("visits_data", conn, if_exists="append", index=False)
    conn.close()
    print("  OK, загружено строк:", len(df))
    return True


def check_table_data(db_path):
    print("[ЭТАП 08] Проверка БД")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM visits_data")
    count = cur.fetchone()[0]
    if count == 0:
        conn.close()
        raise ValueError("Таблица пуста")
    cur.execute("SELECT visit_date, visits, visitors, pageviews FROM visits_data LIMIT 3")
    rows = cur.fetchall()
    conn.close()
    print("  OK, записей:", count)
    print("  Пример данных:")
    for r in rows:
        print("    ", r)
    return True


def run_pipeline():
    print("=" * 50)
    print("ЗАПУСК ПАЙПЛАЙНА")
    print("=" * 50)
    try:
        check_file_exists(RAW_FILE)
        validate_columns(RAW_FILE)
        validate_data_quality(RAW_FILE)
        preprocess_data(RAW_FILE, PROCESSED_FILE)
        check_processed_file(PROCESSED_FILE)
        init_db(DB_PATH)
        load_data_to_db(DB_PATH, PROCESSED_FILE)
        check_table_data(DB_PATH)
        print("=" * 50)
        print("ПАЙПЛАЙН УСПЕШНО ЗАВЕРШЁН")
        print("=" * 50)
    except Exception as e:
        print("ОШИБКА:", e)


if __name__ == "__main__":
    run_pipeline()
