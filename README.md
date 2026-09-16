# Автоматизированная система анализа посещаемости сайта

## Описание проекта

Проект реализует полный цикл работы с данными:
Яндекс Метрика → Python + SQL → Apache Airflow → Yandex DataLens.

## Ссылка на дашборд

[Открыть дашборд в DataLens](https://datalens.yandex/lml99agc2tec4?_share_link=public)

## Содержимое репозитория

- `pipeline.py` — обработка данных (Этап 2): проверка файла, валидация, очистка, загрузка в SQLite
- `dag.py` — DAG для Apache Airflow (Этап 3): 8 задач по расписанию

## Технологии

- **Python** (pandas, sqlite3) — обработка данных
- **SQL** (SQLite) — хранение данных
- **Apache Airflow** — автоматизация
- **Yandex DataLens** — визуализация

## Как запустить

```bash
pip install pandas openpyxl
python3 pipeline.py

