import requests

# URL to fetch JSON data
url = "http://ergast.com/api/f1/circuits.json"

# Fetching JSON data from the URL
response = requests.get(url)

# Check if request was successful (status code 200)
if response.status_code == 200:
    # Convert JSON data to dictionary
    data = response.json()
    
    # Print the dictionary
    print(data)
else:
    print("Failed to fetch data. Status code:", response.status_code)