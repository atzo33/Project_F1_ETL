import requests
from kafka import KafkaProducer
import json

def scraping_circuits_and_publishing_to_kafka(producer):
    circuit_id = 1000
    try:
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

                    # Create a dictionary representing the circuit data
                    circuit = {
                        
                        "circuit_id_json": circuit_id_json,
                        "name_y": name_y,
                        "location": location,
                        "country": country,
                        "lat": lat,
                        "lng": lng
                    }

                    # Publish circuit data to Kafka
                    producer.send('data_topic', {"type":"circuit","data":circuit})
                    print("Published circuit data to Kafka:", name_y)

                    # Increment circuit ID
                    circuit_id += 1

                # Continue processing next race data
                print("Race number is", race_number)
                race_number += 1
            else:
                print("Failed to fetch data from the API.")
                break
    except Exception as error:
        print("Error:", error)

def scraping_drivers_and_publishing_to_kafka(producer):
    
    try:
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

                    # Create a dictionary representing the racer data
                    driver = {
                        "driverRef":driver_ref,
                        "number":number,
                        "code":code,
                        "forename":forename,
                        "surname":surname,
                        "dob":dob,
                        "nationality":nationality
                    }

                    # Publish circuit data to Kafka
                    producer.send('data_topic', value={"type":"driver","data":driver})
                    print("Published driver data to Kafka:", driver)

                    
                   

                # Continue processing next race data
                print("Race number is", race_number)
                race_number += 1
            else:
                print("Failed to fetch data from the API.")
                break
    except Exception as error:
        print("Error:", error)

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Call the function to scrape data and publish to Kafka
scraping_circuits_and_publishing_to_kafka(producer)
scraping_drivers_and_publishing_to_kafka(producer)
