from kafka import KafkaConsumer
import json
import psycopg2 # type: ignore
import requests

# Initialize Kafka consumer
consumer_circuit = KafkaConsumer('circuit_data_topic', bootstrap_servers='localhost:9092',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

print("Consumer circuit is",consumer_circuit)
# Subscribe to the topic


# Continuously consume messages from the topic
def insert_circuit_data(consumer_circuit):
    consumer_circuit.subscribe(['circuit_data_topic'])
    circuit_id = 1000
    conn=None
    race_number = 1
    for message in consumer_circuit:
        
        try:
            conn = psycopg2.connect(
                dbname="f1_database",
                user="airflow",
                password="airflow",
                host="localhost",
                port="5432"
            )
            cursor = conn.cursor()
           
            

            message_data = message.value

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
                
        except Exception as error:
            print("Error:", error)
        finally:
            # Closing database connection
            if conn:
                cursor.close()
                conn.close()
                print("PostgreSQL connection is closed")

       

insert_circuit_data(consumer_circuit)