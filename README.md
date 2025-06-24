# Проект: Анализ данных с использованием PySpark и PostgreSQL

## Содержание

- [Описание](#описание)
- [Задание](#задание)
- [Стек технологий](#cтек_технологий)
- [Установка](#установка)
- [Использование](#использование)

## Описание

Этот проект предназначен для обработки данных о полетах с использованием Apache Airflow и PySpark. Он загружает данные из CSV файла, обрабатывает их, сохраняет в базу данных PostgreSQL и выполняет анализ.

## Задание
1. Загрузите файл данных в DataFrame PySpark. Обязательно выведите количество строк.
2. Убедитесь, что данные корректно прочитаны (правильный формат, отсутствие пустых строк).
3. Преобразуйте текстовые и числовые поля в соответствующие типы данных (например, дата, число).
4. Найдите топ-5 авиалиний с наибольшей средней задержкой.
5. Вычислите процент отмененных рейсов для каждого аэропорта.
6. Определите, какое время суток (утро, день, вечер, ночь) чаще всего связано с задержками рейсов.
7. Добавьте в данные о полетах новые столбцы, рассчитанные на основе существующих данных:
-IS_LONG_HAUL: Флаг, указывающий, является ли рейс дальнемагистральным (если расстояние больше 1000 миль).
-DAY_PART: Определите, в какое время суток происходит вылет (утро, день, вечер, ночь).
8. Создайте схему таблицы в PostgreSQL, которая будет соответствовать структуре ваших данных. PostgreSQL уже находится в docker-compose! Схему нужно создать вне Airflow, например через Dbeaver.
9. Настройте соединение с PostgreSQL из кода, но из PySpark. (обязательно сделать это нужно в Airflow)
10. Загрузите только 10.000 строк из DataFrame в таблицу в PostgreSQL. (обязательно сделать это нужно в Airflow)
11. Выполните SQL скрипт в Python-PySpark скрипте, который выведет компанию - общее время полетов ее самолетов.

## Стек технологий

- Apache Airflow
- PySpark
- PostgreSQL
- Docker

## Установка

1. **Клонируйте репозиторий:**

   ```bash
   git clone https://github.com/andreynetrebin/de_course_final_task_2.git
   cd de_course_final_task_2
   
2. **Создайте и активируйте виртуальное окружение (опционально):**

   ```bash
    python -m venv venv
    source venv/bin/activate  # Для Linux/Mac
    venv\Scripts\activate     # Для Windows

3. **Создайте и активируйте виртуальное окружение (опционально):**

   ```bash
    apache-airflow
    pyspark
    psycopg2-binary
    requests
   
4. **Настройте Docker Compose:**

   ```yaml
    version: '3.7'
    
    x-airflow-common:
      &airflow-common
      build: .
      image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.9.2}
      environment:
        &airflow-common-env
        AIRFLOW__CORE__EXECUTOR: CeleryExecutor
        AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
        AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
        AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0
        AIRFLOW__CORE__FERNET_KEY: ''
        AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
        AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
        AIRFLOW__API__AUTH_BACKENDS: 'airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session'
        AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK: 'true'
        _PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:-}
      volumes:
        - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
        - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
        - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
        - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
      user: "${AIRFLOW_UID:-50000}:0"
      depends_on:
        &airflow-common-depends-on
        redis:
          condition: service_healthy
        postgres:
          condition: service_healthy

5. **Запустите Docker Compose:**

   ```bash
    docker-compose up -d

6. **Запустите Docker Compose:**
- Перейдите в веб-интерфейс Airflow по адресу http://localhost:8080 и активируйте DAG main.

## Использование
- DAG загружает данные о полетах из CSV файла, обрабатывает их и сохраняет в PostgreSQL.
- После завершения обработки, DAG выполняет SQL-запрос для получения общего времени полетов для каждой авиакомпании.