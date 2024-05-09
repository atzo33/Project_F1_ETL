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
    driverStandings_id=80000
    constructorStandings_id=30000
    result_id=30000
    race_ids={}
    
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
            # print("Poruka je")
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
            # If the type of message is driverStandings
            if message.value['type']=="driverStandings":  
                print("USLO U DRIVER-STANDINGS")
                
                message_data = message.value['data']

                driver_id= message_data['driverRef']
                forename = message_data['forename']
                surname = message_data['surname']
                points = message_data['points']
                position = message_data['position']
                wins = message_data['wins']
                race_number=message_data['race_number']

                cursor.execute('SELECT "raceId" FROM race WHERE "year" = %s AND "round" = %s', ("2024", race_number))
                
                race_id_result = cursor.fetchone()
                if race_id_result:
                    race_id = race_id_result[0]
                    print("Race id je ",race_id)
                else:
                    print(f"Race ID not found for race number {race_number}. Exiting loop.")
                    break
                



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
            # If message is constructorStandings
            if message.value['type']=="constructorStandings":  
                print("USLO U CONSTRUCTOR-STANDINGS")
                
                message_data = message.value['data']

                constructor_id= message_data['constructorRef']
                name = message_data['name']
                points = message_data['points']
                position = message_data['position']
                wins = message_data['wins']
                race_number=message_data['race_number']

                cursor.execute('SELECT "raceId" FROM race WHERE "year" = %s AND "round" = %s', ("2024", race_number))
                
                race_id_result = cursor.fetchone()
                
                if race_id_result:
                    race_id = race_id_result[0]
                    print("Race id je ",race_id)
                else:
                    print(f"Race ID not found for race number {race_number}. Exiting loop.")
                    break
                



                # Fetch driverId from the database (case-sensitive)
                cursor.execute('SELECT * FROM constructor WHERE "constructorRef" = %s', (constructor_id,))
                result = cursor.fetchone()
                print("Rezultat pretrage je",result)
                if result:
                    constructor_id_from_db = result[0]

                    # Insert data into driverstandings table
                    cursor.execute("""
                        INSERT INTO constructorstandings ("constructorStandingsId","raceId", "constructorId", "constructorName","points", "position", "wins")
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (constructorStandings_id,race_id, constructor_id_from_db, name, points, position, wins))

                    # Commit changes to the database
                    conn.commit()

                    # Print additional details
                    print("Inserted constructor standings for:", name)
                    constructorStandings_id += 1
                else:
                    print("Driver not found in the database:", driver_id)
                    continue            
              # If message is results         
            if message.value['type']=="results":  
                print("USLO U RESULTS")
                
                message_data = message.value['data']
                driverId= message_data['driverId']
                constructorId=message_data['constructorId']
                carNumber=message_data['car_number']
                positionOrder=message_data['positionOrder']
                points=message_data['points']
                laps=message_data['laps']
                status=message_data['status']
                fastestLap=message_data['fastestLap']
                rankOfFastestLap=message_data['rankOfFastestLap']
                fastestLapTime=message_data['fastestLapTime']
                fastestLapSpeed=message_data['fastestLapSpeed']
                time=message_data['time']
                circuitId=message_data['circuitId']
                raceName=message_data['raceName']
                raceDate=message_data['raceDate']
                race_number=message_data['race_number']
                position=message_data['position']

                cursor.execute('SELECT "raceId" FROM race WHERE "year" = %s AND "round" = %s', ("2024", race_number))

                race_id_result = cursor.fetchone()
                if race_id_result:
                    race_id_map = race_id_result[0]
                    print("Race id je ", race_id_map)
                else:
                    print(f"Race ID not found for race number {race_number}. Exiting loop.")
                    break

               

                
                # Get driver ID from the database
                cursor.execute('SELECT "driverId" FROM driver WHERE "driverRef" = %s', (driverId,))
                driver_id_result = cursor.fetchone()
                if driver_id_result:
                    driver_id_map = driver_id_result[0]
                    print("Driver id je", driver_id_map)
                else:
                    print("Driver not found in the database:", driver_id_map)
                

                # Get constructor ID from the database
                cursor.execute('SELECT "constructorId" FROM constructor WHERE "constructorRef" = %s', (constructorId,))
                constructor_id_result = cursor.fetchone()
                if constructor_id_result:
                    constructor_id_map = constructor_id_result[0]
                    print("Constructor id is",constructor_id_map)
                else:
                    print("Constructor not found in the database:", constructor_id_map)
                   

                # Get driver standings ID
                cursor.execute(
                    'SELECT "driverStandingsId" FROM driverstandings WHERE "raceId" = %s AND "driverId" = %s',
                    (race_id_map, driver_id_map))
                driver_standings_id_result = cursor.fetchone()
                if driver_standings_id_result:
                    driver_standings_id_map = driver_standings_id_result[0]
                    print("Driver standings id is",driver_standings_id_map)
                else:
                    print("Driver standings ID not found in the database.")
                    

                # Get constructor standings ID
                cursor.execute('SELECT "constructorStandingsId" FROM constructorstandings WHERE "raceId" = %s AND "constructorId" = %s', (race_id_map, constructor_id_map))
                constructor_standings_id_result = cursor.fetchone()
                if constructor_standings_id_result:
                    constructor_standings_id_map = constructor_standings_id_result[0]
                    print("Constructor standings id is",constructor_standings_id_map)
                else:
                    print("Constructor standings ID not found in the database.")
                    continue

                # Extract fastest lap details or set to None if 'FastestLap' object is missing
               

                # Insert into results table
                cursor.execute("""
                    INSERT INTO results ("resultId", "raceId", "driverId", "constructorId", "carNumber", "positionOrder", "points", "laps", "time", "fastestLap", "rankOfFastestLap", "fastestLapTime", "fastestLapSpeed", "positionFinish", "driverStandingsId", "constructorStandingsId", "status")
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (result_id, race_id_map, driver_id_map, constructor_id_map, carNumber, positionOrder, points, laps, time, fastestLap, rankOfFastestLap, fastestLapTime, fastestLapSpeed, position, driver_standings_id_map, constructor_standings_id_map, status))

                # Commit changes to the database
                conn.commit()
                print("Rezultat sa id-em",result_id)
                result_id = result_id + 1
            # If message is qualificationOrder
            if message.value['type']=="qualificationOrder":  
                # print("USLO U QUALIFICATION ORDER")
                

                

                message_data = message.value['data']

                driver_id_quali= message_data['driverId']
                forename = message_data['forename']
                surname = message_data['surname']
                constructorName = message_data['constructorName']
                grid = message_data['grid']
                year=message_data['year']
                name_x=message_data['name_x']
                round=message_data['round']

                # Get raceId from the race table
                cursor.execute('SELECT "raceId" FROM race WHERE "year" = %s AND "round" = %s', (2024, round))
                race_id_result = cursor.fetchone()
                if race_id_result:
                    race_id_quali = race_id_result[0]
                    print("Race id is ",race_id_quali)
                else:
                    print(f"Race ID not found for season 2024 and round {round}. Exiting loop.")
                    break
                
                # Get driverId from the driver table
                cursor.execute('SELECT "driverId" FROM driver WHERE "driverRef" = %s', (driver_id_quali,))
                driver_id_result = cursor.fetchone()
                if driver_id_result:
                    driver_id_db = driver_id_result[0]
                else:
                    print(f"Driver ID not found in the database for driverId {driver_id}. Skipping.")
                    continue

                # Insert qualification order info into the qualificationorder table
                cursor.execute("""
                    INSERT INTO qualificationorder ("raceId", "driverId", "forename", "surname", "constructorName", "grid", "year", "name_x")
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (race_id_quali, driver_id_db, forename, surname, constructorName, grid, year, name_x))

                print("Inserted order for",race_id_quali, driver_id_db, forename, surname, constructorName, grid, year, name_x)
                # Commit changes to the database
                conn.commit()

                #if message is pitstops   
            if message.value['type']=="pitstops":  
                # print("USLO U PITSTOPS")
                

                

                message_data = message.value['data']

                driver_id_pits= message_data['driverId']
                stop = message_data['stop']
                pitstopLap = message_data['pitstopLap']
                pitstopTime = message_data['pitstopTime']
                pitstopDuration = message_data['pitstopDuration'] 
                year=message_data['year']
                round=message_data['round']

                # Get raceId from the race table
                cursor.execute('SELECT "raceId" FROM race WHERE "year" = %s AND "round" = %s', (2024, round))
                race_id_result = cursor.fetchone()
                if race_id_result:
                    race_id_pits = race_id_result[0]
                    print("Race id is ",race_id_pits)
                else:
                    print(f"Race ID not found for season 2024 and round {round}. Exiting loop.")
                    break
                
                # Get driver info from the driver table
                cursor.execute('SELECT "driverId", "forename", "surname" FROM driver WHERE "driverRef" = %s', (driver_id_pits,))
                driver_info = cursor.fetchone()
                if driver_info:
                    driver_id_db, forename, surname = driver_info
                else:
                    print(f"Driver not found in the database for driverId {driver_id}. Skipping.")
                    continue
                
                #  Get resultId from the results table
                cursor.execute('SELECT "resultId" FROM results WHERE "raceId" = %s AND "driverId" = %s', (race_id_pits, driver_id_db))
                result_id_result = cursor.fetchone()
                if result_id_result:
                    result_id = result_id_result[0]
                else:
                    print(f"Result ID not found for race ID {race_id_pits} and driver ID {driver_id_db}. Skipping.")
                    continue

                # Insert pit stop info into the pitstops table
                cursor.execute("""
                    INSERT INTO pitstops ("resultId", "raceId", "driverId", "forename", "surname", "stop", "pitstopLap", "pitstopTime", "pitstopDuration")
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (result_id, race_id_pits, driver_id_db, forename, surname, stop, pitstopLap, pitstopTime, pitstopDuration))
                print("Inserted pitstop laps for",result_id, race_id_pits, driver_id_db, forename, surname, stop, pitstopLap, pitstopTime, pitstopDuration)
                # Commit changes to the database
                conn.commit()
            # If message is laps
            if message.value['type']=="laps":  
                # print("USLO U LAPS")




                message_data = message.value['data']

                season= message_data['season']
                round = message_data['raceRound']
                lapNumber = message_data['lapNumber']
                driver_id_lap = message_data['driverId']
                position = message_data['position']
                lapTime=message_data['lapTime']

                if round not in race_ids:
                    cursor.execute('SELECT "raceId" FROM race WHERE "year" = %s AND "round" = %s', ("2024", round))
                    race_id_result = cursor.fetchone()

                    race_ids[round] = race_id_result
                    print("Idijevi su",race_ids)

                cursor.execute('SELECT "driverId", "forename", "surname" FROM driver WHERE "driverRef" = %s', (driver_id_lap,))
                driver_info = cursor.fetchone()
                if driver_info:
                    # print("Info o driveru je",driver_info)
                    driver_id_db, forename, surname = driver_info
                else:
                    print(f"Driver not found in the database for driverId {driver_id}. Skipping.")
                    continue
                race_id_from_dict=race_ids[round][0]
                # print("Race id from dict is",race_id_from_dict)
                # Get resultId from the results table
                cursor.execute('SELECT "resultId" FROM results WHERE "raceId" = %s AND "driverId" = %s', (race_id_from_dict, driver_id_db))
                result_id_result = cursor.fetchone()
                if result_id_result:
                    result_id = result_id_result[0]
                    # print("Result id je",result_id)
                else:
                    print(f"Result ID not found for race ID {race_id} and driver ID {driver_id_db}. Skipping.")
                    continue             

                cursor.execute("""
                                INSERT INTO lapsinfo ("resultId", "raceId", "driverId", "forename", "surname", "lap", "position", "time")
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            """, (result_id, race_id_from_dict, driver_id_db, forename, surname, lapNumber, position, lapTime))

                            # Commit changes to the database
                print("Unesen lap",result_id, race_id_from_dict, driver_id_db, forename, surname, lapNumber, position, lapTime)
                conn.commit()

                     

    except Exception as error:
            print("Error:", error)
    finally:
            # Closing database connection
            if conn:
                cursor.close()
                conn.close()
                print("PostgreSQL connection is closed")


insert_data(consumer)

