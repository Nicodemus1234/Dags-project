DB_CONFIG = {
    "host": "74.249.80.201",
    "port": 5432,
    "dbname": "weather_db",
    "user": "weather_db",
    "password": "1234",
    "schema": "weather_ke from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import json
import pandas as pd
import http.client

# PostgreSQL configuration
DB_CONFIG = {
    "host": "74.249.80.201",
    "port": 5432,
    "dbname": "weather_db",
    "user": "weather_db",
    "password": "1234",
    "schema": "weather_ke"
}

# List of cities and countries to fetch weather for
cities = [("Nairobi", "KE"), ("London", "GB")]

def fetch_and_store_weather():
    """
    Fetches weather data from OpenWeatherMap API and stores it in PostgreSQL
    """
    conn = None
    cursor = None
    try:
        # Connect to the database
        conn = psycopg2.connect(
            dbname=DB_CONFIG["dbname"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"]
        )
        cursor = conn.cursor()

        # Create schema if it doesn't exist
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_CONFIG['schema']}")
        
        # Set the schema search path
        cursor.execute(f"SET search_path TO {DB_CONFIG['schema']}")

        # Create table if it doesn't exist
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {DB_CONFIG['schema']}.weather_data (
                id SERIAL PRIMARY KEY,
                city VARCHAR(100),
                country VARCHAR(100),
                description VARCHAR(100),
                temperature FLOAT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

        # Fetch and store weather for each city
        for city, country in cities:
            params = {
                "q": f"{city},{country}",
                "appid": "e8b6b1536665415231370207991bdf57",
                "units": "metric"
            }

            response = requests.get(
                "https://api.openweathermap.org/data/2.5/weather",
                params=params,
                timeout=10
            )
            data = response.json()

            if response.status_code == 200:
                description = data['weather'][0]['description']
                temperature = data['main']['temp']

                cursor.execute(
                    f"""INSERT INTO {DB_CONFIG['schema']}.weather_data 
                        (city, country, description, temperature)
                        VALUES (%s, %s, %s, %s)
                    """,
                    (city, country, description, temperature)
                )
                print(f"Successfully stored weather for {city}, {country}")
            else:
                print(f"Failed to fetch weather for {city}: {data.get('message')}")

        conn.commit()

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        if conn:
            conn.rollback()
        raise  # Re-raise the exception to mark task as failed in Airflow
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Define the DAG
with DAG(
    dag_id='weather_data_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    description='A pipeline that fetches weather data and stores it in PostgreSQL',
    tags=['weather', 'data_engineering']
) as dag:
    
    fetch_and_store_task = PythonOperator(
        task_id='fetch_and_store_weather_data',
        python_callable=fetch_and_store_weather
    )
