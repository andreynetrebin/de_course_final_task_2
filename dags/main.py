from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, BooleanType
import psycopg2

# Инициализация SparkSession
spark = SparkSession.builder \
    .appName("Flight Data Processing") \
    .getOrCreate()

# 1. Функция для скачивания CSV файлов
def download_file():
    base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?'
    public_key = 'https://disk.yandex.ru/d/TXZjQ6bbuWCo_g'  # Ваша публичная ссылка

    # Получаем информацию о ресурсе
    resource_info_url = f'https://cloud-api.yandex.net/v1/disk/public/resources?public_key={public_key}'
    resource_info_response = requests.get(resource_info_url)

    if resource_info_response.status_code == 200:
        resource_info = resource_info_response.json()
        print("Содержимое по публичной ссылке:")

        # Извлекаем ссылки на файлы
        items = resource_info['_embedded']['items']
        for item in items:
            file_name = item['name']
            if file_name == 'flights_pak.csv':  # Загружаем только flights_pak.csv
                download_url = item['file']
                destination_path = os.path.join('/tmp', file_name)
                response = requests.get(download_url)
                if response.status_code == 200:
                    with open(destination_path, 'wb') as f:
                        f.write(response.content)
                    print(f"Файл {destination_path} успешно загружен.")
                else:
                    print(f"Ошибка при загрузке файла {download_url}: {response.status_code}")

# 2. Функция для загрузки в PySpark и обработки данных
def process_data():
    destination_path = '/tmp/flights_pak.csv'
    
    # Загрузка данных в DataFrame
    df = spark.read.csv(destination_path, header=True, inferSchema=True)
    print(f"Количество строк в DataFrame: {df.count()}")

    # Проверка корректности данных
    if df.filter(F.col("FLIGHT_NUMBER").isNull()).count() == 0:  # Проверка на пустые строки
        print("Данные корректно прочитаны.")
    else:
        print("Обнаружены пустые строки в данных.")

    # Преобразование типов данных
    df = df.withColumn("DATE", F.to_date(F.col("DATE"), "yyyy-MM-dd")) \
           .withColumn("DEPARTURE_DELAY", F.col("DEPARTURE_DELAY").cast(FloatType())) \
           .withColumn("ARRIVAL_DELAY", F.col("ARRIVAL_DELAY").cast(FloatType())) \
           .withColumn("DISTANCE", F.col("DISTANCE").cast(FloatType())) \
           .withColumn("CANCELLED", F.col("CANCELLED").cast(BooleanType()))

    # Топ-5 авиалиний с наибольшей средней задержкой
    top_airlines = df.groupBy("AIRLINE").agg(F.avg("ARRIVAL_DELAY").alias("avg_delay")) \
                     .orderBy(F.desc("avg_delay")).limit(5)
    top_airlines.show()

    # Процент отмененных рейсов для каждого аэропорта
    canceled_flights = df.groupBy("ORIGIN_AIRPORT").agg(
        (F.sum(F.col("CANCELLED").cast(FloatType())) / F.count("*") * 100).alias("cancellation_rate")
    )
    canceled_flights.show()

    # Определение времени суток
    df = df.withColumn("DAY_PART", 
        F.when((F.hour(F.col("DEPARTURE_HOUR")) >= 5) & (F.hour(F.col("DEPARTURE_HOUR")) < 12), "Утро")
        .when((F.hour(F.col("DEPARTURE_HOUR")) >= 12) & (F.hour(F.col("DEPARTURE_HOUR")) < 18), "День")
        .when((F.hour(F.col("DEPARTURE_HOUR")) >= 18) & (F.hour(F.col("DEPARTURE_HOUR")) < 24), "Вечер")
        .otherwise("Ночь"))

    # Добавление новых столбцов
    df = df.withColumn("IS_LONG_HAUL", F.when(F.col("DISTANCE") > 1000, True).otherwise(False))

    # Возвращаем обработанный DataFrame
    return df.limit(10000)  # Возвращаем только 10,000 строк

# 3. Функция для загрузки данных в PostgreSQL
def save_to_postgres(df):
    # Настройка соединения с PostgreSQL
    conn = psycopg2.connect(
        dbname="test",
        user="user",
        password="password",
        host="postgres_user",
    )
    cursor = conn.cursor()

    # Создание таблицы (схема должна быть создана заранее)
    create_table_query = """
    CREATE TABLE IF NOT EXISTS flights (
        flight_id SERIAL PRIMARY KEY,
        airline VARCHAR(255),
        departure_time TIME,
        distance FLOAT,
        canceled BOOLEAN,
        date DATE,
        day_part VARCHAR(50),
        is_long_haul BOOLEAN
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Сохранение данных в таблицу
    for row in df.collect():
        insert_query = """
        INSERT INTO flights (airline, departure_time, distance, canceled, date, day_part, is_long_haul)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        cursor.execute(insert_query, (
            row.AIRLINE, row.DEPARTURE_HOUR, row.DISTANCE, row.CANCELLED, row.DATE, row.DAY_PART, row.IS_LONG_HAUL))

    conn.commit()
    cursor.close()
    conn.close()
    print("Данные успешно сохранены в PostgreSQL.")

# 4. Функция для выполнения SQL-запроса и вывода общего времени полетов
def calculate_total_flight_time():
    # Настройка соединения с PostgreSQL
    conn = psycopg2.connect(
        dbname="test",
        user="user",
        password="password",
        host="postgres_user",
    )
    cursor = conn.cursor()

    # Выполнение SQL-запроса
    query = """
    SELECT airline, SUM(ARRIVAL_DELAY) AS total_flight_time
    FROM flights
    GROUP BY airline;
    """
    cursor.execute(query)
    results = cursor.fetchall()

    # Вывод результатов
    for row in results:
        print(f"Авиакомпания: {row[0]}, Общее время полетов: {row[1]} минут")

    cursor.close()
    conn.close()

with DAG(
        dag_id='main',
        start_date=datetime(2023, 10, 1),
        schedule_interval='@daily',
        catchup=False,
        default_args={
            'execution_timeout': timedelta(minutes=30),  # Установите общий таймаут для всех задач
        }
) as dag:

    # Операторы для выполнения шагов
    download_file_task = PythonOperator(
        task_id='download_file',
        python_callable=download_file,
        dag=dag,
    )
    
    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        dag=dag,
    )
    
    save_to_postgres_task = PythonOperator(
        task_id='save_to_postgres',
        python_callable=lambda: save_to_postgres(process_data_task.output),
        dag=dag,
    )

    calculate_total_flight_time_task = PythonOperator(
        task_id='calculate_total_flight_time',
        python_callable=calculate_total_flight_time,
        dag=dag,
    )

    # Установка зависимостей между задачами
    download_file_task >> process_data_task >> save_to_postgres_task >> calculate_total_flight_time_task

