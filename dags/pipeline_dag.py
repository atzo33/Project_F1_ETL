from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2
import pandas as pd
import requests
import xml.etree.ElementTree as ET

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
            host="praksa_postgres_1",
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
            host="praksa_postgres_1",
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

def read_and_create_dicts():
    try:
        # Read the CSV file into a pandas DataFrame
        # df = pd.read_csv('dataSetPart.csv')
        df = pd.read_csv('dataEngineeringDataset.csv')
        
        # Create dictionaries to store data
        results_dict = {}
        driver_dict={}
        race_dict={}
        circuit_dict={}
        qualiOrder_dict={}
        constructor_dict={}
        constructorStanding_dict={}
        driverStandings_dict={}
        laptimes_dict = {}
        pitstops_dict = {}

        
        # Iterate through each row in the DataFrame and populate dictionaries
        for index, row in df.iterrows():
            result_id = row["resultId"]
            driver_id = row["driverId"]
            lap = row["lap"]
            stop = row["stop"]
            
    #    Napravi ovdje odmah prvih 5 mapi za svaku tabelu po mapu, bice lakse
            # Populate results_dict if the key is unique
            if result_id not in results_dict:
                results_dict[result_id] = {
                    "resultId":row["resultId"],
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

             # Populate driver_dict if the key is unique
            if driver_id not in driver_dict:
                driver_dict[driver_id] = {
                    "driverId":row["driverId"],
                    "driverRef": row["driverRef"],
                    "number_drivers": row["number_drivers"],
                    "code": row["code"],
                    "forename": row["forename"],
                    "surname": row["surname"],
                    "dob": row["dob"],
                    "nationality": row["nationality"]
                }

            if row["constructorId"] not in constructor_dict:
                constructor_dict[row["constructorId"]] = {
                    "constructorId":row["constructorId"],
                    "constructorRef":row["constructorRef"],
                    "name":row["name"],
                    "nationality_constructors":row["nationality_constructors"]
                }

            

            # Populate race_dict if the key is unique
            if row["raceId"] not in race_dict:
                race_dict[row["raceId"]] = {
                    "raceId":row["raceId"],
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
                
            # Populate circuit_dict if the key is unique
            if row["circuitId"] not in circuit_dict:
                circuit_dict[row["circuitId"]] = {
                    "circuitId":row["circuitId"],
                    "circuitRef": row["circuitRef"],
                    "name_y": row["name_y"],
                    "location": row["location"],
                    "country": row["country"],
                    "lat": row["lat"],
                    "lng": row["lng"],
                    "alt": row["alt"]
                }

            # Populate qualiOrder_dict if the key is unique
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
            
            # Populate constructorStanding_dict if the key is unique
            if row["constructorStandingsId"] not in constructorStanding_dict:
                constructorStanding_dict[row["constructorStandingsId"]] = {
                    "constructorStandingsId":row["constructorStandingsId"],
                    "raceId":row["raceId"],
                    "constructorId": row["constructorId"],
                    "constructorRef": row["constructorRef"],
                    "points": row["points_constructorstandings"],
                    "position": row["position_constructorstandings"],
                    "wins": row["wins_constructorstandings"]
                }

            # Populate driverStandings_dict if the key is unique
            if row["driverStandingsId"] not in driverStandings_dict:
                driverStandings_dict[row["driverStandingsId"]] = {
                    "driverStandingsId":row["driverStandingsId"],
                    "raceId":row["raceId"],
                    "driverId": row["driverId"],
                    "forename": row["forename"],
                    "surname": row["surname"],
                    "points": row["points_driverstandings"],
                    "position": row["position_driverstandings"],
                    "wins": row["wins"]
                }
        
            
            # Populate laptimes_dict if the key is unique
            if (result_id, driver_id, lap) not in laptimes_dict:
                laptimes_dict[(result_id, driver_id, lap)] = {
                    "resultId":row["resultId"],
                    "driverId":row["driverId"],
                    "forename": row["forename"],
                    "surname": row["surname"],
                    "lap":row["lap"],
                    "position_laptimes": row["position_laptimes"],
                    "time_laptimes": row["time_laptimes"]
                }
            
            # Populate pitstops_dict if the key is unique
            if (result_id, driver_id, stop) not in pitstops_dict:
                pitstops_dict[(result_id, driver_id, stop)] = {
                    "resultId":row["resultId"],
                    "driverId":row["driverId"],
                    "forename": row["forename"],
                    "surname": row["surname"],
                    "stop":row["stop"],
                    "lap_pitstops": row["lap_pitstops"],
                    "time_pitstops": row["time_pitstops"],
                    "duration": row["duration"]
                }
        
        # Print dictionaries
        print("Results Dictionary:")
        print(results_dict)
        print("\nLaptimes Dictionary:")
        print(laptimes_dict)
        print("\nPitstops Dictionary:")
        print(pitstops_dict)

        # Insert data into the results table
        
        
        insert_driver_data(driver_dict)

        insert_circuit_data(circuit_dict)

        insert_race_data(race_dict)

        insert_constructor_standings_data(constructorStanding_dict)

        insert_driver_standings_data(driverStandings_dict)

        insert_results_data(results_dict)
        
        insert_qualification_order_data(qualiOrder_dict)

        insert_laptimes_data(laptimes_dict)

        insert_pitstops_data(pitstops_dict)

        insert_constructor_data(constructor_dict)

    except Exception as error:
        print("Error:", error)

def insert_results_data(results_dict):
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="praksa_postgres_1",
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
                data["points"],
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

def insert_driver_data(driver_dict):
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="praksa_postgres_1",
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

def insert_circuit_data(circuit_dict):
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="praksa_postgres_1",
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
                data["circuitRef"],
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

def insert_race_data(race_dict):
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="praksa_postgres_1",
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

def insert_qualification_order_data(qualiOrder_dict):

    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="praksa_postgres_1",
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

def insert_constructor_standings_data(constructorStanding_dict):
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="praksa_postgres_1",
            port="5432"
        )
        
        # Create a cursor object using the cursor() method
        cursor = conn.cursor()

        # Iterate through each entry in the constructorStanding dictionary and insert into the constructorstandings table
        for constructorStandingsId, data in constructorStanding_dict.items():
            cursor.execute("""
                INSERT INTO constructorstandings ("constructorStandingsId","raceId","constructorId", "constructorName", "points", "position", "wins")
                VALUES (%s,%s, %s, %s, %s, %s, %s)
            """, (
                data["constructorStandingsId"],
                data["raceId"],
                data["constructorId"],
                data["constructorRef"],
                data["points"],
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

def insert_driver_standings_data(driverStandings_dict):
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="praksa_postgres_1",
            port="5432"
        )
        
        # Create a cursor object using the cursor() method
        cursor = conn.cursor()

        # Iterate through each entry in the driverStandings dictionary and insert into the driverstandings table
        for driverStandingsId, data in driverStandings_dict.items():
            cursor.execute("""
                INSERT INTO driverstandings ("driverStandingsId","raceId", "driverId", "forename", "surname", "points", "position", "wins")
                VALUES (%s, %s, %s, %s, %s, %s, %s,%s)
            """, (
                data["driverStandingsId"],
                data["raceId"],
                data["driverId"],
                data["forename"],
                data["surname"],
                data["points"],
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

def insert_laptimes_data(laptimes_dict):
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="praksa_postgres_1",
            port="5432"
        )
        
        # Create a cursor object using the cursor() method
        cursor = conn.cursor()

        # Iterate through each entry in the laptimes dictionary and insert into the laptimes table
        for key, data in laptimes_dict.items():
            cursor.execute("""
                INSERT INTO lapsinfo ("resultId", "driverId", "forename", "surname", "lap", "position", "time")
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                data["resultId"],
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

def insert_pitstops_data(pitstops_dict):
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="praksa_postgres_1",
            port="5432"
        )
        
        # Create a cursor object using the cursor() method
        cursor = conn.cursor()


        # Iterate through each entry in the pitstops dictionary and insert into the pitstops table
        for key, data in pitstops_dict.items():
            pitstop_duration=data["duration"]


            if not isinstance(pitstop_duration, float):
                pitstop_duration = None
            cursor.execute("""
                INSERT INTO pitstops ("resultId", "driverId", "forename", "surname", "stop", "pitstopLap", "pitstopTime", "pitstopDuration")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data["resultId"],
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

def insert_constructor_data(constructor_dict):
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="praksa_postgres_1",
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

# Inserting scraped data into existing tables in database
def scraping_data_and_loading_circuits():
    circuit_id = 1000
    try:
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="praksa_postgres_1",
            port="5432"
        )
        cursor = conn.cursor()

        race_number = 1

        while True:
            url = f"http://ergast.com/api/f1/2024/{race_number}/circuits.json"
            response = requests.get(url)
            data = response.json()

            if 'MRData' in data and 'CircuitTable' in data['MRData']:
                circuits = data['MRData']['CircuitTable']['Circuits']

                if not circuits:  # Check if circuits data is empty
                    print(f"No circuit data found for the given year and race number {race_number}. Exiting loop.")
                    break

                for circuit_data in circuits:
                    circuit_id_json = circuit_data['circuitId']
                    name_y = circuit_data['circuitName']
                    location = circuit_data['Location']['locality']
                    country = circuit_data['Location']['country']
                    lat = circuit_data['Location']['lat']
                    lng = circuit_data['Location']['long']

                    # Check if circuit exists in the database
                    cursor.execute('SELECT * FROM circuit WHERE "name_y" = %s', (name_y,))
                    result = cursor.fetchone()
                    if not result:
                        # Insert new circuit into the database
                        cursor.execute("""
                            INSERT INTO circuit ("circuitId", "name_x", "name_y", "location", "country", "lat", "lng")
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (circuit_id, circuit_id_json, name_y, location, country, lat, lng))

                        # Commit changes to the database
                        conn.commit()

                        circuit_id += 1

                        # Print additional details
                        print("Inserted new circuit:", name_y)

                # Continue processing next race data
                print("Race number is", race_number)
                race_number += 1
            else:
                print("Failed to fetch data from the API.")
                break
    except Exception as error:
        print("Error:", error)
    finally:
        # Closing database connection
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")

def scraping_data_and_loading_drivers():
    driver_Id=1000
    try:
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="praksa_postgres_1",
            port="5432"
        )
        cursor = conn.cursor()

        race_number = 1

        while True:
            url = f"http://ergast.com/api/f1/2024/{race_number}/drivers.json"
            response = requests.get(url)
            data = response.json()

            if 'MRData' in data and 'DriverTable' in data['MRData']:
                drivers = data['MRData']['DriverTable']['Drivers']

                if not drivers:  # Check if drivers data is empty
                    print(f"No driver data found for the given year and race number {race_number}. Exiting loop.")
                    break

                for driver_data in drivers:
                    
                    driver_ref = driver_data['driverId']
                    number = driver_data['permanentNumber']
                    code = driver_data['code']
                    forename = driver_data['givenName']
                    surname = driver_data['familyName']
                    dob = driver_data['dateOfBirth']
                    nationality = driver_data['nationality']

                    # Check if driver exists in the database
                    cursor.execute('SELECT * FROM driver WHERE "driverRef" = %s', (driver_ref,))
                    result = cursor.fetchone()
                    if not result:
                        # Insert new driver into the database
                        cursor.execute("""
                            INSERT INTO driver ("driverId", "driverRef", "number", "code", "forename", "surname", "dob", "nationality")
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """, (driver_Id,driver_ref, number, code, forename, surname, dob, nationality))

                        # Commit changes to the database
                        conn.commit()

                        driver_Id=driver_Id+1

                        # Print additional details
                        print("Inserted new driver:", forename, surname)

                # Continue processing next race data
                print("Race number is", race_number)
                race_number += 1
            else:
                print("Failed to fetch data from the API.")
                break
    except Exception as error:
        print("Error:", error)
    finally:
        # Closing database connection
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")

def scraping_data_and_loading_constructors():
    constructor_Id=1000
    try:
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="praksa_postgres_1",
            port="5432"
        )
        cursor = conn.cursor()

        race_number = 1

        while True:
            url = f"http://ergast.com/api/f1/2024/{race_number}/constructors.json"
            response = requests.get(url)
            data = response.json()

            if 'MRData' in data and 'ConstructorTable' in data['MRData']:
                constructors = data['MRData']['ConstructorTable']['Constructors']

                if not constructors:  # Check if constructors data is empty
                    print(f"No constructor data found for the given year and race number {race_number}. Exiting loop.")
                    break

                for constructor_data in constructors:
                    
                    constructor_ref = constructor_data['constructorId']
                    name = constructor_data['name']
                    nationality = constructor_data['nationality']

                    # Check if constructor exists in the database
                    cursor.execute('SELECT * FROM constructor WHERE "constructorRef" = %s', (constructor_ref,))
                    result = cursor.fetchone()
                    if not result:
                        # Insert new constructor into the database
                        cursor.execute("""
                            INSERT INTO constructor ("constructorId","constructorRef", "name", "nationality")
                            VALUES (%s, %s, %s,%s)
                        """, (constructor_Id,constructor_ref, name, nationality))

                        # Commit changes to the database
                        conn.commit()
                        constructor_Id=constructor_Id+1
                        

                        # Print additional details
                        print("Inserted new constructor:", name)

                # Continue processing next race data
                print("Race number is", race_number)
                race_number += 1
            else:
                print("Failed to fetch data from the API.")
                break
    except Exception as error:
        print("Error:", error)
    finally:
        # Closing database connection
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")

def scraping_data_and_loading_racetable():
    race_Id=5000
    try:
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="praksa_postgres_1",
            port="5432"
        )
        cursor = conn.cursor()
        
        
        race_number = 1
       
        
        while True:
            url = f"http://ergast.com/api/f1/2024/{race_number}/results.json"
            response = requests.get(url)
            data = response.json()
            
            if 'MRData' in data and 'RaceTable' in data['MRData']:
                race_table = data['MRData']['RaceTable']
                race_data = race_table.get('Races', [])

                if not race_data:  # Check if "Races" is empty
                    print(f"No race data found for the given year and race number {race_number}. Exiting loop.")
                    break
                
                # Extract additional details
                season = race_table['season']
                round_number = race_table['round']
                race_date = race_data[0]['date']
                race_time = race_data[0]['time']
                circuit_name = race_data[0]['Circuit']['circuitName']

                print("Season:", season)
                print("Round:", round_number)
                print("Date:", race_date)
                print("Time:", race_time)
                print("Circuit name",circuit_name)
                print("Race id je",race_Id)
                    

                cursor.execute("SELECT * FROM circuit WHERE name_y = %s", (circuit_name,))
                circuit_id = cursor.fetchone()
                
                if circuit_id:
                    circuit_id = circuit_id[0]
                    

                    cursor.execute("""
                            INSERT INTO race ("raceId", "circuitId", "year", "round", "date", "time")
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (
                                race_Id,
                                circuit_id,
                                season,
                                round_number,
                                race_date,
                                race_time
                            ))
                    

                    # Commit changes to the database
                    
                    conn.commit()

                    # Print additional details
                    print("Season:", season)
                    print("Round:", round_number)
                    print("Date:", race_date)
                    print("Time:", race_time)
                    print("Circuit name",circuit_name)
                    
                    # Continue processing race data
                    print("Race number is", race_number)
                    race_number += 1
                    print("Race id is", race_Id)
                    race_Id=race_Id+1
                    
                else:
                    print("Circuit not found:", circuit_name)
                    break
            else:
                print("Failed to fetch data from the API.")
                break
    except Exception as error:
        print("Error:", error)
    
    finally:
        # Closing database connection
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")

def scraping_data_and_loading_driverstandings():
    driverStandings_id = 80000
    try:
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="praksa_postgres_1",
            port="5432"
        )
        cursor = conn.cursor()

        race_number = 1

        while True:
            url = f"http://ergast.com/api/f1/2024/{race_number}/driverStandings.json"
            response = requests.get(url)
            data = response.json()

            if 'MRData' in data and 'StandingsTable' in data['MRData']:
                standings = data['MRData']['StandingsTable']['StandingsLists'][0]['DriverStandings']

                if not standings:  # Check if standings data is empty
                    print(f"No driver standings data found for the given year and race number {race_number}. Exiting loop.")
                    break

                cursor.execute('SELECT "raceId" FROM race WHERE "year" = %s AND "round" = %s', ("2024", race_number))
                
                race_id_result = cursor.fetchone()
                if race_id_result:
                    race_id = race_id_result[0]
                    print("Race id je ",race_id)
                else:
                    print(f"Race ID not found for race number {race_number}. Exiting loop.")
                    break


                for driver_data in standings:
                    driver_id = driver_data['Driver']['driverId']
                    forename = driver_data['Driver']['givenName']
                    surname = driver_data['Driver']['familyName']
                    points = int(driver_data['points'])
                    position = int(driver_data['position'])
                    wins = int(driver_data['wins'])

                    # Fetch driverId from the database (case-sensitive)
                    cursor.execute('SELECT * FROM driver WHERE "driverRef" = %s', (driver_id,))
                    result = cursor.fetchone()
                    if result:
                        driver_id_from_db = result[0]

                        # Insert data into driverstandings table
                        cursor.execute("""
                            INSERT INTO driverstandings ("driverStandingsId","raceId", "driverId", "forename", "surname", "points", "position", "wins")
                            VALUES (%s, %s, %s, %s, %s, %s, %s,%s)
                        """, (driverStandings_id,race_id, driver_id_from_db, forename, surname, points, position, wins))

                        # Commit changes to the database
                        conn.commit()

                        # Print additional details
                        print("Inserted driver standings for:", forename, surname)
                        driverStandings_id += 1
                    else:
                        print("Driver not found in the database:", driver_id)
                        continue

                # Continue processing next race data
                print("Race number is", race_number)
                race_number += 1
            else:
                print("Failed to fetch data from the API.")
                break
    except Exception as error:
        print("Error:", error)
    finally:
        # Closing database connection
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")

def scraping_data_and_loading_constructorstandings():
    constructorStandings_id = 90000
    try:
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="praksa_postgres_1",
            port="5432"
        )
        cursor = conn.cursor()

        race_number = 1

        while True:
            url = f"http://ergast.com/api/f1/2024/{race_number}/constructorstandings.json"
            response = requests.get(url)
            data = response.json()

            if 'MRData' in data and 'StandingsTable' in data['MRData']:
                standings = data['MRData']['StandingsTable']['StandingsLists'][0]['ConstructorStandings']

                if not standings:  # Check if standings data is empty
                    print(f"No constructor standings data found for the given year and race number {race_number}. Exiting loop.")
                    break


                cursor.execute('SELECT "raceId" FROM race WHERE "year" = %s AND "round" = %s', ("2024", race_number))
                
                race_id_result = cursor.fetchone()
                if race_id_result:
                    race_id = race_id_result[0]
                    print("Race id je ",race_id)
                else:
                    print(f"Race ID not found for race number {race_number}. Exiting loop.")
                    break
            

                for constructor_data in standings:
                    constructor_id = constructor_data['Constructor']['constructorId']
                    constructor_name = constructor_data['Constructor']['name']
                    points = int(constructor_data['points'])
                    position = int(constructor_data['position'])
                    wins = int(constructor_data['wins'])

                    # Fetch constructorId from the database (case-sensitive)
                    cursor.execute('SELECT * FROM constructor WHERE "constructorRef" = %s', (constructor_id,))
                    result = cursor.fetchone()
                    if result:
                        constructor_id_from_db = result[0]

                        # Insert data into constructorstandings table
                        cursor.execute("""
                            INSERT INTO constructorstandings ("constructorStandingsId","raceId", "constructorId", "constructorName", "points", "position", "wins")
                            VALUES (%s, %s, %s, %s, %s, %s,%s)
                        """, (constructorStandings_id,race_id, constructor_id_from_db, constructor_name, points, position, wins))

                        # Commit changes to the database
                        conn.commit()

                        # Print additional details
                        print("Inserted constructor standings for:", constructor_name)
                        constructorStandings_id += 1
                    else:
                        print("Constructor not found in the database:", constructor_id)
                        continue

                # Continue processing next race data
                print("Race number is", race_number)
                race_number += 1
            else:
                print("Failed to fetch data from the API.")
                break
    except Exception as error:
        print("Error:", error)
    finally:
        # Closing database connection
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")


def insert_race_results():
    try:
        conn = psycopg2.connect(
            dbname="f1_database",
            user="airflow",
            password="airflow",
            host="praksa_postgres_1",
            port="5432"
        )
        cursor = conn.cursor()

        result_id = 30000
        race_number = 1

        while True:
            url = f"http://ergast.com/api/f1/2024/{race_number}/results.json"
            response = requests.get(url)
            data = response.json()

            if 'MRData' in data and 'RaceTable' in data['MRData']:
                race_data = data['MRData']['RaceTable']['Races'][0]
                race_name = race_data['raceName']
                race_date = race_data['date']

                # Extract race time if available
                race_time = race_data['Results'][0]['Time'].get('time', None) if race_data['Results'] else None

                circuit_id = race_data['Circuit']['circuitId']

                # Get the race ID from the races database
                cursor.execute('SELECT "raceId" FROM race WHERE "year" = %s AND "round" = %s', ("2024", race_number))

                race_id_result = cursor.fetchone()
                if race_id_result:
                    race_id = race_id_result[0]
                    print("Race id je ", race_id)
                else:
                    print(f"Race ID not found for race number {race_number}. Exiting loop.")
                    break

                results = race_data['Results']

                for result in results:
                    driver_id_json = result['Driver']['driverId']
                    constructor_id_json = result['Constructor']['constructorId']
                    car_number = result['number']
                    position_order = result['grid']
                    points = result['points']
                    laps = result['laps']
                    status = result['status']

                    # Get driver ID from the database
                    cursor.execute('SELECT "driverId" FROM driver WHERE "driverRef" = %s', (driver_id_json,))
                    driver_id_result = cursor.fetchone()
                    if driver_id_result:
                        driver_id = driver_id_result[0]
                        print("Driver id je", driver_id)
                    else:
                        print("Driver not found in the database:", driver_id_json)
                        continue

                    # Get constructor ID from the database
                    cursor.execute('SELECT "constructorId" FROM constructor WHERE "constructorRef" = %s', (constructor_id_json,))
                    constructor_id_result = cursor.fetchone()
                    if constructor_id_result:
                        constructor_id = constructor_id_result[0]
                    else:
                        print("Constructor not found in the database:", constructor_id_json)
                        continue

                    # Get driver standings ID
                    cursor.execute(
                        'SELECT "driverStandingsId" FROM driverstandings WHERE "raceId" = %s AND "driverId" = %s',
                        (race_id, driver_id))
                    driver_standings_id_result = cursor.fetchone()
                    if driver_standings_id_result:
                        driver_standings_id = driver_standings_id_result[0]
                    else:
                        print("Driver standings ID not found in the database.")
                        continue

                    # Get constructor standings ID
                    cursor.execute('SELECT "constructorStandingsId" FROM constructorstandings WHERE "raceId" = %s AND "constructorId" = %s', (race_id, constructor_id))
                    constructor_standings_id_result = cursor.fetchone()
                    if constructor_standings_id_result:
                        constructor_standings_id = constructor_standings_id_result[0]
                    else:
                        print("Constructor standings ID not found in the database.")
                        continue

                    # Extract fastest lap details or set to None if 'FastestLap' object is missing
                    fastest_lap = result.get('FastestLap', None)
                    if fastest_lap:
                        fastest_lap_lap = fastest_lap.get('lap', None)
                        rank_of_fastest_lap = fastest_lap.get('rank', None)
                        fastest_lap_time = fastest_lap['Time'].get('time', None)
                        fastest_lap_speed = fastest_lap['AverageSpeed'].get('speed', None)
                    else:
                        fastest_lap_lap = rank_of_fastest_lap = fastest_lap_time = fastest_lap_speed = None

                    # Insert into results table
                    cursor.execute("""
                        INSERT INTO results ("resultId", "raceId", "driverId", "constructorId", "carNumber", "positionOrder", "points", "laps", "time", "fastestLap", "rankOfFastestLap", "fastestLapTime", "fastestLapSpeed", "positionFinish", "driverStandingsId", "constructorStandingsId", "status")
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (result_id, race_id, driver_id, constructor_id, car_number, position_order, points, laps, race_time, fastest_lap_lap, rank_of_fastest_lap, fastest_lap_time, fastest_lap_speed, result['position'], driver_standings_id, constructor_standings_id, status))

                    # Commit changes to the database
                    conn.commit()

                    result_id = result_id + 1

                    print("Inserted race result for:", race_name, "Driver:", result['Driver']['givenName'], result['Driver']['familyName'])
                race_number = race_number + 1
                # Continue processing next race data
                print("Race number is", race_number)
                print("Result_id is", result_id)

            else:
                print("Failed to fetch data from the API.")
                break
    except Exception as error:
        print("Error:", error)
    finally:
        # Closing database connection
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")


with DAG('etlPipeline', 
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

    insert_data_task = PythonOperator(
    task_id='insert_data_task',
    python_callable=read_and_create_dicts,
    
    )
    insert_scraped_data_task = PythonOperator(
    task_id='insert_scraped_data_task',
    python_callable=scraping_data_and_loading_racetable,
    
    )
    insert_scraped_driverstandings_task = PythonOperator(
    task_id='insert_scraped_driverstandings_task',
    python_callable=scraping_data_and_loading_driverstandings,
    
    )
    insert_scraped_constructorstandings_task = PythonOperator(
    task_id='insert_scraped_constructorstandings_task',
    python_callable=scraping_data_and_loading_constructorstandings,
    
    )

    insert_scraped_drivers_task = PythonOperator(
    task_id='insert_scraped_drivers_task',
    python_callable=scraping_data_and_loading_drivers,
    
    )

    insert_scraped_constructors_task = PythonOperator(
    task_id='insert_scraped_constructors_task',
    python_callable=scraping_data_and_loading_constructors,
    
    )

    insert_scraped_circuits_task = PythonOperator(
    task_id='insert_scraped_circuits_task',
    python_callable=scraping_data_and_loading_circuits,
    
    )

    insert_scraped_results_task = PythonOperator(
    task_id='insert_scraped_results_task',
    python_callable=insert_race_results,
    
    )

    







    

    drop_tables_task >> create_tables_task >> insert_data_task >> insert_scraped_circuits_task >> [insert_scraped_drivers_task,insert_scraped_constructors_task] >> insert_scraped_data_task >> insert_scraped_driverstandings_task >> insert_scraped_constructorstandings_task >> insert_scraped_results_task
