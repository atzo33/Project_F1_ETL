from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2
import pandas as pd
import csv

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 19),
    'retries': 1
}

def drop_tables():
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="project_f1_etl_postgres_1",
            port="5432"
        )
        
        # Create a cursor object using the cursor() method
        cursor = conn.cursor()
        
        # SQL query to drop all tables
        drop_table_queries = [
            'DROP TABLE IF EXISTS "results" CASCADE;',
            'DROP TABLE IF EXISTS "circuit" CASCADE;',
            'DROP TABLE IF EXISTS "race" CASCADE;',
            'DROP TABLE IF EXISTS "driver" CASCADE;',
            'DROP TABLE IF EXISTS "constructor" CASCADE;',
            'DROP TABLE IF EXISTS "driverstandings" CASCADE;',
            'DROP TABLE IF EXISTS "constructorstandings" CASCADE;',
            'DROP TABLE IF EXISTS "lapsinfo" CASCADE;',
            'DROP TABLE IF EXISTS "qualificationorder" CASCADE;'
            'DROP TABLE IF EXISTS "pitstops" CASCADE;'
        ]
        
        # Execute SQL commands: drop tables
        for query in drop_table_queries:
            cursor.execute(query)
        
        conn.commit()
        print("Tables dropped successfully in PostgreSQL")

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while dropping PostgreSQL tables:", error)

    finally:
        # closing database connection.
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")

def create_tables():
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="project_f1_etl_postgres_1",
            port="5432"
        )
        
        # Create a cursor object using the cursor() method
        cursor = conn.cursor()
        
        # SQL queries to create tables
        create_table_queries = [
            '''CREATE TABLE IF NOT EXISTS "results" (
                   "resultId" INT PRIMARY KEY,
                   "raceId" INT,
                   "driverId" INT,
                   "constructorId" INT,
                   "carNumber" INT,
                   "positionOrder" INT,
                   "points" INT,
                   "laps" INT,
                   "time" VARCHAR,
                   "fastestLap" INT,
                   "rankOfFastestLap" INT,
                   "fastestLapTime" TIME,
                   "fastestLapSpeed" FLOAT,
                   "positionFinish" INT,
                   "driverStandingsId" INT,
                   "constructorStandingsId" INT,
                   "status" VARCHAR
            );''',
            
            '''CREATE TABLE IF NOT EXISTS "circuit" (
                   "circuitId" INT PRIMARY KEY,
                   "name_x" VARCHAR,
                   "name_y" VARCHAR,
                   "location" VARCHAR,
                   "country" VARCHAR,
                   "lat" FLOAT,
                   "lng" FLOAT,
                   "alt" FLOAT
            );''',
            
            '''CREATE TABLE IF NOT EXISTS "race" (
                   "raceId" INT PRIMARY KEY,
                   "circuitId" INT,
                   "year" INT,
                   "round" INT,
                   "fp1Date" DATE,
                   "fp1Time" TIME,
                   "fp2Date" DATE,
                   "fp2Time" TIME,
                   "fp3Date" DATE,
                   "fp3Time" TIME,
                   "qualiDate" DATE,
                   "qualiTime" TIME,
                   "sprintDate" DATE,
                   "sprintTime" TIME,
                   "date" DATE,
                   "time" TIME,
                   FOREIGN KEY ("circuitId") REFERENCES "circuit" ("circuitId")
            );''',
            
            '''CREATE TABLE IF NOT EXISTS "driver" (
                   "driverId" INT PRIMARY KEY,
                   "driverRef" VARCHAR,
                   "number" VARCHAR,
                   "code" VARCHAR,
                   "forename" VARCHAR,
                   "surname" VARCHAR,
                   "dob" DATE,
                   "nationality" VARCHAR
            );''',
            
            '''CREATE TABLE IF NOT EXISTS "constructor" (
                   "constructorId" INT PRIMARY KEY,
                   "constructorRef" VARCHAR,
                   "name" VARCHAR,
                   "nationality" VARCHAR
            );''',
            
            '''CREATE TABLE IF NOT EXISTS "driverstandings" (
                   "driverStandingsId" INT PRIMARY KEY,
                   "raceId" INT,
                   "driverId" INT,
                   "forename" VARCHAR,
                   "surname" VARCHAR,
                   "points" INT,
                   "position" INT,
                   "wins" INT
            );''',
            
            '''CREATE TABLE IF NOT EXISTS "constructorstandings" (
                   "constructorStandingsId" INT PRIMARY KEY,
                   "raceId" INT,
                   "constructorId" INT,
                   "constructorName" VARCHAR,
                   "points" INT,
                   "position" INT,
                   "wins" INT
            );''',
            
            '''CREATE TABLE IF NOT EXISTS "lapsinfo" (
                   "resultId" INT,
                   "raceId" INT,
                   "driverId" INT,
                   "forename" VARCHAR,
                   "surname" VARCHAR,
                   "lap" INT,
                   "position" INT,
                   "time" TIME,                  
                   FOREIGN KEY ("resultId") REFERENCES "results" ("resultId")
            );''',
            
            '''CREATE TABLE IF NOT EXISTS "qualificationorder" (
                   "raceId" INT,
                   "driverId" INT,
                   "forename" VARCHAR,
                   "surname" VARCHAR,
                   "constructorName" VARCHAR,
                   "grid" INT,
                   "year" INT,
                   "name_x" VARCHAR,
                   FOREIGN KEY ("raceId") REFERENCES "race" ("raceId")
            );'''

            '''CREATE TABLE IF NOT EXISTS "pitstops" (
                "resultId" INT,
                "raceId" INT,
                "driverId" INT,
                "forename" VARCHAR,
                "surname" VARCHAR,
                "stop" INT,
                "pitstopLap" INT,
                "pitstopTime" TIME,
                "pitstopDuration" FLOAT,
                FOREIGN KEY ("resultId") REFERENCES "results" ("resultId")
            );'''

        ]
        
        # Execute SQL commands: create tables
        for query in create_table_queries:
            cursor.execute(query)
        
        conn.commit()
        print("Tables created successfully in PostgreSQL")

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while creating PostgreSQL tables:", error)

    finally:
        # closing database connection.
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")
def etl_results():
    try:
        # Open the CSV file
        with open('dataEngineeringDataset.csv', mode='r') as file:
            # Create a CSV reader
            reader = csv.DictReader(file)
            
            # Create dictionary to store results
            results_dict = {}
            
            # Process each row
            for row in reader:
                result_id = row["resultId"]
                if result_id not in results_dict:
                    results_dict[result_id] = {
                        "resultId": row["resultId"],
                        "raceId": row["raceId"],
                        "driverId": row["driverId"],
                        "constructorId": row["constructorId"],
                        "carNumber": row["number"],
                        "number_drivers": row["number_drivers"],
                        "positionOrder": row["positionOrder"],
                        "code": row["code"],
                        "forename": row["forename"],
                        "surname": row["surname"],
                        "dob": row["dob"],
                        "nationality": row["nationality"],
                        "points": row["points"],
                        "laps": row["laps"],
                        "time": row["time"],
                        "fastestLap": row["fastestLap"],
                        "rank": row["rank"],
                        "fastestLapTime": row["fastestLapTime"],
                        "fastestLapSpeed": row["fastestLapSpeed"],
                        "positionFinish": row["position"],
                        "driverStandingsId": row["driverStandingsId"],
                        "constructorStandingsId": row["constructorStandingsId"],
                        "status": row["status"],
                        "driverRef": row["driverRef"]
                    }

    except Exception as error:
        print("Error:", error)
    

    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="project_f1_etl_postgres_1",
            port="5432"
        )
        
        # Create a cursor object using the cursor() method
        cursor = conn.cursor()

        for result_id, data in results_dict.items():
            # Replace '\N' with None for nullable fields
            for field in ["time", "fastestLapTime", "fastestLapSpeed","carNumber","fastestLap","rank","positionFinish"]:
                if data[field] == '\\N':
                    data[field] = None
        
        # Iterate through each result in the results dictionary and insert into the results table
        for result_id, data in results_dict.items():
            points=int(float(data["points"]))
            cursor.execute("""
                INSERT INTO results ("resultId", "raceId", "driverId", "constructorId", "carNumber", "positionOrder", "points", "laps", "time", "fastestLap", "rankOfFastestLap", "fastestLapTime", "fastestLapSpeed", "positionFinish", "driverStandingsId", "constructorStandingsId", "status")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data["resultId"],
                data["raceId"],
                data["driverId"],
                data["constructorId"],
                data["carNumber"],
                data["positionOrder"],
                points,
                data["laps"],
                data["time"],
                data["fastestLap"],
                data["rank"],
                data["fastestLapTime"],
                data["fastestLapSpeed"],
                data["positionFinish"],
                data["driverStandingsId"],
                data["constructorStandingsId"],
                data["status"]
            ))
        
        # Commit the transaction
        conn.commit()
        print("Data inserted successfully into PostgreSQL")

    except Exception as error:
        print("Error:", error)
    
    finally:
        # Closing database connection
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")

def etl_driver():
    try:
        # Open the CSV file
        with open('dataEngineeringDataset.csv', mode='r') as file:
            # Create a CSV reader
            reader = csv.DictReader(file)
            
            # Create dictionary to store driver data
            driver_dict = {}
            
            # Process each row
            for row in reader:
                driver_id = row["driverId"]
                if driver_id not in driver_dict:
                    driver_dict[driver_id] = {
                        "driverId": row["driverId"],
                        "driverRef": row["driverRef"],
                        "number_drivers": row["number_drivers"],
                        "code": row["code"],
                        "forename": row["forename"],
                        "surname": row["surname"],
                        "dob": row["dob"],
                        "nationality": row["nationality"]
                    }

    except Exception as error:
        print("Error:", error)
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="project_f1_etl_postgres_1",
            port="5432"
        )
        
        # Create a cursor object using the cursor() method
        cursor = conn.cursor()
        for driver_id, data in driver_dict.items():
            # Replace '\N' with None for nullable fields
            for field in ["number_drivers", "code", "dob"]:
                if data[field] == '\\N':
                    data[field] = None
        
        # Iterate through each result in the results dictionary and insert into the driver table
        for driver_id, data in driver_dict.items():
            cursor.execute("""
                INSERT INTO driver ("driverId", "driverRef", "number", "code", "forename", "surname", "dob", "nationality")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data["driverId"], 
                data["driverRef"],
                data["number_drivers"],
                data["code"],
                data["forename"],
                data["surname"],
                data["dob"],
                data["nationality"]
            ))
        
        # Commit the transaction
        conn.commit()
        print("Driver data inserted successfully into PostgreSQL")

    except Exception as error:
        print("Error:", error)
    
    finally:
        # Closing database connection
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")

def etl_race():
    try:
        # Open the CSV file
        with open('dataEngineeringDataset.csv', mode='r') as file:
            # Create a CSV reader
            reader = csv.DictReader(file)
            
            # Create a dictionary to store race data
            race_dict = {}
            
            # Process each row
            for row in reader:
                race_id = row["raceId"]
                if race_id not in race_dict:
                    race_dict[race_id] = {
                        "raceId": row["raceId"],
                        "circuitId": row["circuitId"],
                        "year": row["year"],
                        "round": row["round"],
                        "fp1Date": row["fp1_date"],
                        "fp1Time": row["fp1_time"],
                        "fp2Date": row["fp2_date"],
                        "fp2Time": row["fp2_time"],
                        "fp3Date": row["fp3_date"],
                        "fp3Time": row["fp3_time"],
                        "qualiDate": row["quali_date"],
                        "qualiTime": row["quali_time"],
                        "sprintDate": row["sprint_date"],
                        "sprintTime": row["sprint_time"],
                        "date": row["date"],
                        "time": row["time_races"]
                    }

    except Exception as error:
        print("Error:", error)

    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="project_f1_etl_postgres_1",
            port="5432"
        )
        
        # Create a cursor object using the cursor() method
        cursor = conn.cursor()

        for race_id, data in race_dict.items():
            # Replace '\N' with None for date fields
            for field in ["fp1Date", "fp1Time", "fp2Date", "fp2Time", "fp3Date", "fp3Time",
                          "qualiDate", "qualiTime", "sprintDate", "sprintTime", "date", "time"]:
                if data[field] == '\\N':
                    data[field] = None
        
        # Iterate through each race in the race dictionary and insert into the race table
        for race_id, data in race_dict.items():
            cursor.execute("""
                INSERT INTO race ("raceId", "circuitId", "year", "round", "fp1Date", "fp1Time", "fp2Date", "fp2Time", "fp3Date", "fp3Time", "qualiDate", "qualiTime", "sprintDate", "sprintTime", "date", "time")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data["raceId"],
                data["circuitId"],
                data["year"],
                data["round"],
                data["fp1Date"],
                data["fp1Time"],
                data["fp2Date"],
                data["fp2Time"],
                data["fp3Date"],
                data["fp3Time"],
                data["qualiDate"],
                data["qualiTime"],
                data["sprintDate"],
                data["sprintTime"],
                data["date"],
                data["time"]
            ))
        
        # Commit the transaction
        conn.commit()
        print("Data inserted successfully into PostgreSQL")

    except Exception as error:
        print("Error:", error)
    
    finally:
        # Closing database connection
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")

def etl_circuit():
    try:
        # Open the CSV file
        with open('dataEngineeringDataset.csv', mode='r') as file:
            # Create a CSV reader
            reader = csv.DictReader(file)
            
            # Create a dictionary to store circuit data
            circuit_dict = {}
            
            # Process each row
            for row in reader:
                circuit_id = row["circuitId"]
                if circuit_id not in circuit_dict:
                    circuit_dict[circuit_id] = {
                        "circuitId": row["circuitId"],
                        "name_x": row["name_x"],
                        "name_y": row["name_y"],
                        "location": row["location"],
                        "country": row["country"],
                        "lat": row["lat"],
                        "lng": row["lng"],
                        "alt": row["alt"]
                    }

    except Exception as error:
        print("Error:", error)
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="project_f1_etl_postgres_1",
            port="5432"
        )
        
        # Create a cursor object using the cursor() method
        cursor = conn.cursor()

        for circuit_id, data in circuit_dict.items():
            # Replace '\N' with None for any fields with potentially invalid data
            for field in ["lat", "lng", "alt"]:
                if data[field] == '\\N':
                    data[field] = None
        
        # Iterate through each circuit in the circuit dictionary and insert into the circuit table
        for circuit_id, data in circuit_dict.items():
            cursor.execute("""
                INSERT INTO circuit ("circuitId", "name_x", "name_y", "location", "country", "lat", "lng", "alt")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data["circuitId"],
                data["name_x"],
                data["name_y"],
                data["location"],
                data["country"],
                data["lat"],
                data["lng"],
                data["alt"]
            ))
        
        # Commit the transaction
        conn.commit()
        print("Data inserted successfully into PostgreSQL")

    except Exception as error:
        print("Error:", error)
    
    finally:
        # Closing database connection
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")

def etl_qualiOrder():
    try:
        
        df = pd.read_csv('dataEngineeringDataset.csv')

        qualiOrder_dict={}

        for index, row in df.iterrows():
            result_id = row["resultId"]
            driver_id = row["driverId"]
            lap = row["lap"]
            stop = row["stop"]

            if ((row["raceId"],row["grid"])) not in qualiOrder_dict:
                qualiOrder_dict[(row["raceId"],row["grid"])] = {
                    "raceId":row["raceId"],
                    "driverId":row["driverId"],
                    "forename": row["forename"],
                    "surname": row["surname"],
                    "constructorName": row["constructorRef"],
                    "grid": row["grid"],
                    "year" : row["year"],
                    "name_x":row["name_x"]
                }
    except Exception as error:
        print("Error:", error)

    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="project_f1_etl_postgres_1",
            port="5432"
        )
        
        # Create a cursor object using the cursor() method
        cursor = conn.cursor()

        # Iterate through each entry in the qualiOrder dictionary and insert into the qualificationorder table
        for key, data in qualiOrder_dict.items():
            cursor.execute("""
                INSERT INTO qualificationorder ("raceId", "driverId", "forename", "surname", "constructorName", "grid","year","name_x")
                VALUES (%s, %s, %s, %s, %s, %s,%s,%s)
            """, (
                data["raceId"],
                data["driverId"],
                data["forename"],
                data["surname"],
                data["constructorName"],
                data["grid"],
                data["year"],
                data["name_x"]
            ))
        
        # Commit the transaction
        conn.commit()
        print("Data inserted successfully into PostgreSQL")

    except Exception as error:
        print("Error:", error)
    
    finally:
        # Closing database connection
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")
    
def etl_constructor():
    try:
        # Open the CSV file
        with open('dataEngineeringDataset.csv', mode='r') as file:
            # Create a CSV reader
            reader = csv.DictReader(file)
            
            # Create a dictionary to store constructor data
            constructor_dict = {}
            
            # Process each row
            for row in reader:
                constructor_id = row["constructorId"]
                if constructor_id not in constructor_dict:
                    constructor_dict[constructor_id] = {
                        "constructorId": row["constructorId"],
                        "constructorRef": row["constructorRef"],
                        "name": row["name"],
                        "nationality_constructors": row["nationality_constructors"]
                    }

    except Exception as error:
        print("Error:", error)

    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="project_f1_etl_postgres_1",
            port="5432"
        )
        
        # Create a cursor object using the cursor() method
        cursor = conn.cursor()

        # Iterate through each entry in the constructor dictionary and insert into the constructors table
        for constructor_id, data in constructor_dict.items():
            # Replace '\N' with None for nullable fields
            for field in ["nationality_constructors"]:
                if data[field] == '\\N':
                    data[field] = None
            
            cursor.execute("""
                INSERT INTO constructor ("constructorId", "constructorRef", "name", "nationality")
                VALUES (%s, %s, %s, %s)
            """, (
                data["constructorId"],
                data["constructorRef"],
                data["name"],
                data["nationality_constructors"]
            ))
        
        # Commit the transaction
        conn.commit()
        print("Constructor data inserted successfully into PostgreSQL")

    except Exception as error:
        print("Error:", error)
    
    finally:
        # Closing database connection
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")
    
def etl_constructorStanding():
    try:
        # Open the CSV file
        with open('dataEngineeringDataset.csv', mode='r') as file:
            # Create a CSV reader
            reader = csv.DictReader(file)
            
            # Create a dictionary to store constructor standing data
            constructorStanding_dict = {}
            
            # Process each row
            for row in reader:
                standings_id = row["constructorStandingsId"]
                if standings_id not in constructorStanding_dict:
                    constructorStanding_dict[standings_id] = {
                        "constructorStandingsId": row["constructorStandingsId"],
                        "constructorId": row["constructorId"],
                        "constructorRef": row["constructorRef"],
                        "points": row["points_constructorstandings"],
                        "position": row["position_constructorstandings"],
                        "wins": row["wins_constructorstandings"]
                    }

    except Exception as error:
        print("Error:", error)

    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="project_f1_etl_postgres_1",
            port="5432"
        )
        
        # Create a cursor object using the cursor() method
        cursor = conn.cursor()

        # Iterate through each entry in the constructorStanding dictionary and insert into the constructorstandings table
        for constructorStandingsId, data in constructorStanding_dict.items():
            points=int(float(data["points"]))
            cursor.execute("""
                INSERT INTO constructorstandings ("constructorStandingsId", "constructorId", "constructorName", "points", "position", "wins")
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                data["constructorStandingsId"],
                data["constructorId"],
                data["constructorRef"],
                points,
                data["position"],
                data["wins"]
            ))
        
        # Commit the transaction
        conn.commit()
        print("Construction standings data inserted successfully into PostgreSQL")

    except Exception as error:
        print("Error:", error)
    
    finally:
        # Closing database connection
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")

def etl_driverStandings():
    try:
        # Open the CSV file
        with open('dataEngineeringDataset.csv', mode='r') as file:
            # Create a CSV reader
            reader = csv.DictReader(file)
            
            # Create a dictionary to store driver standings data
            driverStandings_dict = {}
            
            # Process each row
            for row in reader:
                
                standings_id = row["driverStandingsId"]
                if standings_id not in driverStandings_dict:
                    driverStandings_dict[standings_id] = {
                        "driverStandingsId": row["driverStandingsId"],
                        "raceId":row["raceId"],
                        "driverId": row["driverId"],
                        "forename": row["forename"],
                        "surname": row["surname"],
                        "points": row["points_driverstandings"],
                        "position": row["position_driverstandings"],
                        "wins": row["wins"]
                    }
            print("Driverstandings mapa je",driverStandings_dict)
    except Exception as error:
        print("Error:", error)

    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="project_f1_etl_postgres_1",
            port="5432"
        )
        
        # Create a cursor object using the cursor() method
        cursor = conn.cursor()

        # Iterate through each entry in the driverStandings dictionary and insert into the driverstandings table
        for driverStandingsId, data in driverStandings_dict.items():
            points=int(float(data["points"]))
            cursor.execute("""
                INSERT INTO driverstandings ("driverStandingsId","raceId", "driverId", "forename", "surname", "points", "position", "wins")
                VALUES (%s, %s, %s, %s, %s, %s, %s ,%s)
            """, (
                data["driverStandingsId"],
                data["raceId"],
                data["driverId"],
                data["forename"],
                data["surname"],
                points,
                data["position"],
                data["wins"]
            ))
        
        # Commit the transaction
        conn.commit()
        print("Driver standings data inserted successfully into PostgreSQL")

    except Exception as error:
        print("Error:", error)
    
    finally:
        # Closing database connection
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")

def etl_laptimes():
    try:
        # Open the CSV file
        with open('dataEngineeringDataset.csv', mode='r') as file:
            # Create a CSV reader
            reader = csv.DictReader(file)
            
            # Create a dictionary to store lap times data
            laptimes_dict = {}
            
            # Process each row
            for row in reader:
                key = (row["resultId"], row["driverId"], row["lap"])
                if key not in laptimes_dict:
                    laptimes_dict[key] = {
                        "resultId": row["resultId"],
                        "raceId":row["raceId"],
                        "driverId": row["driverId"],
                        "forename": row["forename"],
                        "surname": row["surname"],
                        "lap": row["lap"],
                        "position_laptimes": row["position_laptimes"],
                        "time_laptimes": row["time_laptimes"]
                    }

    except Exception as error:
        print("Error:", error)
    
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="project_f1_etl_postgres_1",
            port="5432"
        )
        
        # Create a cursor object using the cursor() method
        cursor = conn.cursor()

        # Iterate through each entry in the laptimes dictionary and insert into the laptimes table
        for key, data in laptimes_dict.items():
            cursor.execute("""
                INSERT INTO lapsinfo ("resultId","raceId", "driverId", "forename", "surname", "lap", "position", "time")
                VALUES (%s,%s, %s, %s, %s, %s, %s, %s)
            """, (
                data["resultId"],
                data["raceId"],
                data["driverId"],
                data["forename"],
                data["surname"],
                data["lap"],
                data["position_laptimes"],
                data["time_laptimes"]
            ))
        
        # Commit the transaction
        conn.commit()
        print("Laptimes data inserted successfully into PostgreSQL")

    except Exception as error:
        print("Error:", error)
    
    finally:
        # Closing database connection
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")

def etl_pitstops():
    try:
        # Open the CSV file
        with open('dataEngineeringDataset.csv', mode='r') as file:
            # Create a CSV reader
            reader = csv.DictReader(file)
            
            # Create a dictionary to store pit stops data
            pitstops_dict = {}
            
            # Process each row
            for row in reader:
                key = (row["resultId"], row["driverId"], row["stop"])
                if key not in pitstops_dict:
                    pitstops_dict[key] = {
                        "resultId": row["resultId"],
                        "raceId":row["raceId"],
                        "driverId": row["driverId"],
                        "forename": row["forename"],
                        "surname": row["surname"],
                        "stop": row["stop"],
                        "lap_pitstops": row["lap_pitstops"],
                        "time_pitstops": row["time_pitstops"],
                        "duration": row["duration"]
                    }

    except Exception as error:
        print("Error:", error)

    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="project_f1_etl_postgres_1",
            port="5432"
        )
        
        # Create a cursor object using the cursor() method
        cursor = conn.cursor()

        # for key, data in pitstops_dict.items():
        #     # Handling the case where the duration format is invalid
        #     pitstop_duration = data["duration"]
        #     try:
        #         # Try to convert the duration to float
        #         pitstop_duration = float(pitstop_duration)
        #     except ValueError:
        #         # If conversion fails, set duration to NULL
        #         pitstop_duration = None

        # Iterate through each entry in the pitstops dictionary and insert into the pitstops table
        for key, data in pitstops_dict.items():
            
            

            print("Trajanje pitstopa prije parsa",data["duration"])
    
            pitstop_duration=data["duration"]
            
            if not isinstance(pitstop_duration, float):
                        try:
                            pitstop_duration = float(pitstop_duration)
                        except ValueError:
                            pitstop_duration = None
            
            print("Trajanje pitstopa nakon parsa",pitstop_duration)
            cursor.execute("""
                INSERT INTO pitstops ("resultId","raceId", "driverId", "forename", "surname", "stop", "pitstopLap", "pitstopTime", "pitstopDuration")
                VALUES (%s, %s,%s, %s, %s, %s, %s, %s, %s)
            """, (
                data["resultId"],
                data["raceId"],
                data["driverId"],
                data["forename"],
                data["surname"],
                data["stop"],
                data["lap_pitstops"],
                data["time_pitstops"],
                pitstop_duration
            ))
        
        # Commit the transaction
        conn.commit()
        print("Pitstops data inserted successfully into PostgreSQL")

    except Exception as error:
        print("Error:Puca pitstop data", error)
    
    finally:
        # Closing database connection
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")

    



with DAG('atzovPipeline2', 
         default_args=default_args,
         schedule_interval=None) as dag:

    drop_tables_task = PythonOperator(
        task_id='drop_tables_task',
        python_callable=drop_tables
    )

    create_tables_task = PythonOperator(
        task_id='create_tables_task',
        python_callable=create_tables
    )

    etl_pitstops_task = PythonOperator(
        task_id='etl_pitstops_task',
        python_callable=etl_pitstops
    )

    etl_laptimes_task = PythonOperator(
        task_id='etl_laptimes_task',
        python_callable=etl_laptimes
    )

    etl_driverStandings_task = PythonOperator(
        task_id='etl_driverStandings_task',
        python_callable=etl_driverStandings
    )

    etl_constructorStanding_task = PythonOperator(
        task_id='etl_constructorStanding_task',
        python_callable=etl_constructorStanding
    )

    etl_constructor_task = PythonOperator(
        task_id='etl_constructor_task',
        python_callable=etl_constructor
    )

    etl_qualiOrder_task = PythonOperator(
        task_id='etl_qualiOrder_task',
        python_callable=etl_qualiOrder
    )

    etl_circuit_task = PythonOperator(
        task_id='etl_circuit_task',
        python_callable=etl_circuit
    )

    etl_race_task = PythonOperator(
        task_id='etl_race_task',
        python_callable=etl_race
    )

    etl_driver_task = PythonOperator(
        task_id='etl_driver_task',
        python_callable=etl_driver
    )

    etl_results_task = PythonOperator(
        task_id='etl_results_task',
        python_callable=etl_results
    )

    drop_tables_task >> create_tables_task >> etl_circuit_task >>[etl_driver_task,etl_race_task,etl_constructorStanding_task,etl_constructor_task,etl_driverStandings_task ] >> etl_results_task>>[
        etl_pitstops_task,
        etl_laptimes_task,
        etl_qualiOrder_task,
        ]