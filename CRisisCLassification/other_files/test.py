import json
import urllib.request

service_root_URI = 'https://beaware.server.de/SensorThingsService/v1.0/'

SensorThingEntities = ['Things', 'Location', 'HistoricalLocations',
                        'Datastreams', 'Sensor', 'Observation',
                        'ObservedProperties', 'FeaturesOfInterest', 'MultiDatastreams']

resource_path = service_root_URI + SensorThingEntities[0]

things_property = 'properties/type'
things_value = '%27' + 'river' + '%27'

query = resource_path + '?$filter=' + things_property + '%20eq%20' + things_value

print(service_root_URI)
print(resource_path)
print(query)

# read from url - execute the query and the response is stored to json obj

with urllib.request.urlopen(query) as url:
    response = json.loads(url.read().decode())
    print(response)

# write json (data) to output file
with open('outjson_response.txt', 'w') as outfile:
    json.dump(response, outfile)

#print("response_2 name:", response_2['name'])
#print("response_2 id:", response_2['@iot.id'])
#print("Datastream name:", response_2["Datastreams"][0]["name"])
#print("Datastream description:", response_2["Datastreams"][0]["description"])
#print("Datastream ID:", response_2["Datastreams"][0]["@iot.id"])
#print("1st Observation:", response_2["Datastreams"][0]["Observations"][0])


