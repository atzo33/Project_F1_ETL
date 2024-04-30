import requests

def scrape_circuit_data(year, race_number):
    url = f"http://ergast.com/api/f1/{year}/{race_number}/circuits.json"
    response = requests.get(url)
    data = response.json()
    return data

def main():
    year = 2024
    race_number = 1
    circuit_data = scrape_circuit_data(year, race_number)
    circuits = circuit_data['MRData']['CircuitTable']['Circuits']
    
    print("Circuit Information:")
    for circuit in circuits:
        print(f"Name: {circuit['circuitName']}")
        print(f"Location: {circuit['Location']['locality']}, {circuit['Location']['country']}")
        print(f"Latitude: {circuit['Location']['lat']}")
        print(f"Longitude: {circuit['Location']['long']}")
        print()

if __name__ == "__main__":
    main()
