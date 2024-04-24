import requests
import xml.etree.ElementTree as ET

def xml_to_dict(element):
    result = {}
    for child in element:
        if child:
            child_result = xml_to_dict(child)
            if child.tag in result:
                if isinstance(result[child.tag], list):
                    result[child.tag].append(child_result)
                else:
                    result[child.tag] = [result[child.tag], child_result]
            else:
                result[child.tag] = child_result
        else:
            result[child.tag] = child.text
    return result

def parse_f1_results(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        root = ET.fromstring(response.content)
        return xml_to_dict(root)
    else:
        print("Failed to fetch data from the API")
        return None

# API URL
api_url = "http://ergast.com/api/f1/2024/results"

# Parsing F1 results from XML to dictionary
f1_results = parse_f1_results(api_url)
print(f1_results)