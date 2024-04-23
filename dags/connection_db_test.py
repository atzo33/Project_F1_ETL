from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 19),
    'retries': 1
}

def create_table():
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="testdb",
            user="airflow",
            password="airflow",
            host="praksa_postgres_1",
            port="5432"
        )
        
        # Create a cursor object using the cursor() method
        cursor = conn.cursor()
        
        # SQL query to create a table
        create_table_query = '''CREATE TABLE IF NOT EXISTS your_table_name 
                                (ID INT PRIMARY KEY     NOT NULL,
                                NAME           TEXT    NOT NULL); '''
        
        # Execute a SQL command: create table
        cursor.execute(create_table_query)
        conn.commit()
        print("Table created successfully in PostgreSQL ")

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while creating PostgreSQL table", error)

    finally:
        # closing database connection.
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")


with DAG('create_postgres_table', 
         default_args=default_args,
         schedule_interval=None) as dag:

    create_table_task = PythonOperator(
        task_id='create_table_task',
        python_callable=create_table
    )

create_table_task
