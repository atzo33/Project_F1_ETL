from kafka import KafkaConsumer
import json
import psycopg2 # type: ignore
import requests

# Initialize Kafka consumer
consumer_circuit = KafkaConsumer('data_topic', bootstrap_servers='localhost:9092',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))


print("Consumer circuit is",consumer_circuit)

# Subscribe to the topic





# Continuously consume messages from the topic
def insert_circuit_data(consumer_circuit):
    # consumer_circuit.subscribe(['data_topic'])
    
    conn=None
    race_number = 1
    
    try:
        conn = psycopg2.connect(
                    dbname="f1_database",
                    user="airflow",
                    password="airflow",
                    host="localhost",
                    port="5432"
                )
        cursor = conn.cursor()
        
        for message in consumer_circuit:
            print("Poruka je",message.value)
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
                driver_id = 1000
                

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

                # Continue processing next race data
                

            
    except Exception as error:
            print("Error:", error)
    finally:
            # Closing database connection
            if conn:
                cursor.close()
                conn.close()
                print("PostgreSQL connection is closed")

insert_circuit_data(consumer_circuit)

