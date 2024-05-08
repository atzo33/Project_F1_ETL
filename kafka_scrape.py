import time
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


def scraping_constructors_and_publishing_to_kafka(producer):
    
    try:
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
                    constructor = {    
                            "constructor_ref":constructor_ref,
                            "name": name,
                            "nationality":nationality,    
                    }
                    producer.send('data_topic', {"type":"constructor","data":constructor})
                    print("Published constructor data to Kafka:", constructor)

                # Continue processing next race data
                print("Race number is", race_number)
                race_number += 1
            else:
                print("Failed to fetch data from the API.")
                break
    except Exception as error:
        print("Error:", error)

def scraping_race_and_publishing_to_kafka(producer):
    
    try:
        race_number = 1
        url = f"http://ergast.com/api/f1/2024.json"
        response = requests.get(url)
        data = response.json()
        
        if 'MRData' in data and 'RaceTable' in data['MRData']:
            race_table = data['MRData']['RaceTable']
            race_data = race_table.get('Races', [])

            if not race_data:  # Check if "Races" is empty
                print(f"No race data found for the given year and race number {race_number}. Exiting loop.")
                
            
            for race in race_data:
                season = race['season']
                round_number = race['round']
                race_date = race['date']
                race_time = race['time']
                circuit_name = race['Circuit']['circuitName']
                fp1_date = race['FirstPractice']['date']
                fp1_time = race['FirstPractice']['time']
                fp2_date = race.get('SecondPractice', {}).get('date', '')
                fp2_time = race.get('SecondPractice', {}).get('time', '')
                fp3_date = race.get('ThirdPractice', {}).get('date', '')
                fp3_time = race.get('ThirdPractice', {}).get('time', '')
                quali_date = race['Qualifying']['date']
                quali_time = race['Qualifying']['time']
                sprint_date = race.get('Sprint', {}).get('date', '')
                sprint_time = race.get('Sprint', {}).get('time', '')

                race_object = {
                    "season": season,
                    "round": round_number,
                    "fp1Date": fp1_date,
                    "fp1Time": fp1_time,
                    "fp2Date": fp2_date,
                    "fp2Time": fp2_time,
                    "fp3Date": fp3_date,
                    "fp3Time": fp3_time,
                    "qualiDate": quali_date,
                    "qualiTime": quali_time,
                    "sprintDate": sprint_date,
                    "sprintTime": sprint_time,
                    "date": race_date,
                    "time": race_time,
                    "circuitName": circuit_name
                }

                producer.send('data_topic', {"type":"race","data":race_object},partition=0)
                producer.flush()
                # time.sleep(0.01)
                print("Published constructor data to Kafka:", race_object)

                # Now you can use the race_object as you wish, such as publishing it to Kafka
                # For example:
                # producer.publish_race(race_object)

                # print("Race:", race_object)

            race_number += 1
                
    except Exception as error:
        print("Error:", error)

def scraping_driverstandings_and_publishing_to_kafka(producer):
    
    try:
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

                
                
                
                

                for driver_data in standings:
                    driver_id = driver_data['Driver']['driverId']
                    forename = driver_data['Driver']['givenName']
                    surname = driver_data['Driver']['familyName']
                    points = int(driver_data['points'])
                    position = int(driver_data['position'])
                    wins = int(driver_data['wins'])

                    # Fetch driverId from the database (case-sensitive)
                    
                    driverStandings = {
                    "driverRef": driver_id,
                    "forename": forename,
                    "surname": surname,
                    "points": points,
                    "position": position,
                    "wins": wins,
                    "race_number":race_number
                   
                    }

                    producer.send('data_topic', {"type":"driverStandings","data":driverStandings},partition=0)
                    producer.flush()
                    # time.sleep(0.01)
                    print("Published constructor data to Kafka:", driverStandings)
                    

                # Continue processing next race data
                print("Race number is", race_number)
                race_number += 1
            else:
                print("Failed to fetch data from the API.")
                break
    except Exception as error:
        print("Error:", error)

def scraping_constructorstandings_and_publishing_to_kafka(producer):
    
    try:
        race_number = 1
        while True:
            url = f"http://ergast.com/api/f1/2024/{race_number}/constructorstandings.json"
            response = requests.get(url)
            data = response.json()

            if 'MRData' in data and 'StandingsTable' in data['MRData']:
                standings = data['MRData']['StandingsTable']['StandingsLists'][0]['ConstructorStandings']

                if not standings:  # Check if standings data is empty
                    print(f"No driver standings data found for the given year and race number {race_number}. Exiting loop.")
                    break

                
                
                
                

                for constructor_data in standings:
                    constructor_id = constructor_data['Constructor']['constructorId']
                    constructor_name = constructor_data['Constructor']['name']
                    points = int(constructor_data['points'])
                    position = int(constructor_data['position'])
                    wins = int(constructor_data['wins'])

                    # Fetch driverId from the database (case-sensitive)
                    
                    constructorStandings = {
                    "constructorRef": constructor_id,
                    "name": constructor_name,
                    "points": points,
                    "position": position,
                    "wins": wins,
                    "race_number":race_number
                   
                    }

                    producer.send('data_topic', {"type":"constructorStandings","data":constructorStandings},partition=0)
                    producer.flush()
                    # time.sleep(0.01)
                    print("Published constructor data to Kafka:", constructorStandings)
                    

                # Continue processing next race data
                print("Race number is", race_number)
                race_number += 1
            else:
                print("Failed to fetch data from the API.")
                break
    except Exception as error:
        print("Error:", error)

def scraping_results_and_publishing_to_kafka(producer):
    
    try:
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
                

                circuit_id = race_data['Circuit']['circuitId']

                # Get the race ID from the races database
               
                results = race_data['Results']
                # race_time = result['Time'].get('time', None) if race_data['Results'] else None
                # print("rezultati su",results)
                for result in results:
                    race_time = result['Time'].get('time', None) if 'Time' in result else None
                    driver_id_json = result['Driver']['driverId']
                    constructor_id_json = result['Constructor']['constructorId']
                    car_number = result['number']
                    position_order = result['grid']
                    position=result['position']
                    points = result['points']
                    laps = result['laps']
                    status = result['status']

                    
                    # Extract fastest lap details or set to None if 'FastestLap' object is missing
                    fastest_lap = result.get('FastestLap', None)
                    if fastest_lap:
                        fastest_lap_lap = fastest_lap.get('lap', None)
                        rank_of_fastest_lap = fastest_lap.get('rank', None)
                        fastest_lap_time = fastest_lap['Time'].get('time', None)
                        fastest_lap_speed = fastest_lap['AverageSpeed'].get('speed', None)
                    else:
                        fastest_lap_lap = rank_of_fastest_lap = fastest_lap_time = fastest_lap_speed = None

                    
                    results={
                        "driverId":driver_id_json,
                        "constructorId":constructor_id_json,
                        "car_number":car_number,
                        "positionOrder":position_order,
                        "points":points,
                        "laps":laps,
                        "status":status,
                        "fastestLap":fastest_lap_lap,
                        "rankOfFastestLap":rank_of_fastest_lap,
                        "fastestLapTime":fastest_lap_time,
                        "fastestLapSpeed":fastest_lap_speed,
                        "time":race_time,
                        "circuitId":circuit_id,
                        "raceName":race_name,
                        "raceDate":race_date,
                        "race_number":race_number,
                        "position":position
                    }

                    producer.send('data_topic', {"type":"results","data":results},partition=0)
                    print("Race time is ",race_time)
                    producer.flush()

                    # Commit changes to the database
                    

                 

                   
                race_number = race_number + 1
                # Continue processing next race data
                print("Race number is", race_number)
               
            else:
                print("Failed to fetch data from the API.")
                break
    except Exception as error:
        print("Error:", error)
        
# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Call the function to scrape data and publish to Kafka
# scraping_circuits_and_publishing_to_kafka(producer)
# scraping_drivers_and_publishing_to_kafka(producer)
# scraping_constructors_and_publishing_to_kafka(producer)
# scraping_race_and_publishing_to_kafka(producer)
# scraping_driverstandings_and_publishing_to_kafka(producer)
# scraping_constructorstandings_and_publishing_to_kafka(producer)
scraping_results_and_publishing_to_kafka(producer)
