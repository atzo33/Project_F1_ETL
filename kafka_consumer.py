from kafka import KafkaConsumer
import json
import psycopg2 # type: ignore
import requests

# Initialize Kafka consumer
consumer = KafkaConsumer('data_topic', bootstrap_servers='localhost:9092',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')),auto_offset_reset="latest")


print("Consumer circuit is",consumer)

# Continuously consume messages from the topic
def insert_data(consumer):
    consumer.subscribe(['data_topic'])
    
    conn=None
    driver_id = 1000
    constructor_Id = 300
    race_number = 1
    race_Id = 1500
    
    try:
        conn = psycopg2.connect(
                    dbname="f1_database",
                    user="airflow",
                    password="airflow",
                    host="localhost",
                    port="5432"
                )
        cursor = conn.cursor()
        
        for message in consumer:
            print("Poruka je")
            #If type of message is circuit
             
            if message.value['type']=="circuit":
                print("USLO U CIRCUIT")
                circuit_id = 1000 
                message_data = message.value['data']

                # Access fields within the message data
                circuitRef = message_data['circuit_id_json']
                name = message_data['name_y']
                location = message_data['location']
                country = message_data['country']
                lat = message_data['lat']
                lng = message_data['lng']
                
                # Check if circuit exists in the database
                cursor.execute('SELECT * FROM circuit WHERE "name_y" = %s', (name,))
                result = cursor.fetchone()
                
                if not result:
                    # Insert new circuit into the database
                    cursor.execute("""
                        INSERT INTO circuit ("circuitId", "name_x", "name_y", "location", "country", "lat", "lng")
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (circuit_id, circuitRef,name, location, country,lat , lng))

                    # Commit changes to the database
                    conn.commit()

                    circuit_id += 1

                    # Print additional details
                    print("Inserted new circuit:", name)

                # Continue processing next race data
                print("Race number is", race_number)
                race_number += 1


            #If type of message is driver    
            if message.value['type']=="driver":  
                print("USLO U DRIVER")
                
                

                message_data = message.value['data']
                print("Podaci su",message_data)
                # Access fields within the message data
                driver_ref = message_data['driverRef']
                number = message_data['number']
                code = message_data['code']
                forename = message_data['forename']
                surname = message_data['surname']
                dob = message_data['dob']
                nationality = message_data['nationality']

                # Check if circuit exists in the database
                cursor.execute('SELECT * FROM driver WHERE "driverRef" = %s', (driver_ref,))
                result = cursor.fetchone()
                
                if not result:
                    # Insert new circuit into the database
                    cursor.execute("""
                                    INSERT INTO driver ("driverId", "driverRef", "number", "code", "forename", "surname", "dob", "nationality")
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                """, (driver_id,driver_ref, number, code, forename, surname, dob, nationality))

                    # Commit changes to the database
                    conn.commit()

                    driver_id += 1

                    # Print additional details
                    print("Inserted new driver:", driver_ref)

            # If the type of kafka message is constructor

            if message.value['type']=="constructor":  
                print("USLO U CONSTRUCTOR")

                
                

                message_data = message.value['data']
                # print("Podaci su",message_data)
                # Access fields within the message data
                constructor_ref = message_data['constructor_ref']
                name = message_data['name']
                nationality = message_data['nationality']
                

                # Check if circuit exists in the database
                cursor.execute('SELECT * FROM constructor WHERE "constructorRef" = %s', (constructor_ref,))
                result = cursor.fetchone()
                if not result:
                    print("Uslo u no result",name)
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
            # If the type of message is race
            if message.value['type'] == "race":  
                print("USLO U Race")

                
                

                message_data = message.value['data']

                print("Podaci su",message_data)
                # Access fields within the message data
                year = message_data['season']
                round = message_data['round']
                fp1Date = message_data.get('fp1Date') if message_data.get('fp1Date') != '' else None
                fp1Time = message_data.get('fp1Time') if message_data.get('fp1Time') != '' else None
                fp2Date = message_data.get('fp2Date') if message_data.get('fp2Date') != '' else None
                fp2Time = message_data.get('fp2Time') if message_data.get('fp2Time') != '' else None
                fp3Date = message_data.get('fp3Date') if message_data.get('fp3Date') != '' else None
                fp3Time = message_data.get('fp3Time') if message_data.get('fp3Time') != '' else None
                qualiDate = message_data.get('qualiDate') if message_data.get('qualiDate') != '' else None
                qualiTime = message_data.get('qualiTime') if message_data.get('qualiTime') != '' else None
                sprintDate = message_data.get('sprintDate') if message_data.get('sprintDate') != '' else None
                sprintTime = message_data.get('sprintTime') if message_data.get('sprintTime') != '' else None
                date = message_data['date']
                time = message_data['time']
                circuitName=message_data['circuitName']
                
                cursor.execute("SELECT * FROM race WHERE year = %s AND round = %s", (year,round,))
                race_exists=cursor.fetchone()
                print("Trka postoji",race_exists)
                if not race_exists:
                # Check if circuit exists in the database
                    cursor.execute("SELECT * FROM circuit WHERE name_y = %s", (circuitName,))
                    circuit_id = cursor.fetchone()
                    print("Ime staze je",circuit_id)
                    if circuit_id:
                        circuit_id = circuit_id[0]

                        cursor.execute("""
                            INSERT INTO race ("raceId", "circuitId", "year", "round", "fp1Date", "fp1Time", "fp2Date", "fp2Time", "fp3Date", "fp3Time", "qualiDate", "qualiTime", "sprintDate", "sprintTime", "date", "time")
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,(
                                    race_Id,
                                    circuit_id,
                                    year,
                                    round,
                                    fp1Date,
                                    fp1Time,
                                    fp2Date,
                                    fp2Time,
                                    fp3Date,
                                    fp3Time,
                                    qualiDate,
                                    qualiTime,
                                    sprintDate,
                                    sprintTime,
                                    date,
                                    time
                                ))
                        
                        conn.commit()

                        # Print additional details
                        print("Inserted new race:", race_Id)
                        race_Id=race_Id+1
                

    except Exception as error:
            print("Error:", error)
    finally:
            # Closing database connection
            if conn:
                cursor.close()
                conn.close()
                print("PostgreSQL connection is closed")


insert_data(consumer)

