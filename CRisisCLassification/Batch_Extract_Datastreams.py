
#----------------------------------------------------------------------------------------
# Extract attributes of the stations of the river sections from SensorThingServer API
#
#filter_vals = 'station'  # 'river'
#sel_vals = ['description','id','name','properties']
#SensorThings = [SensorThingEntities[0]] # Things

#response_stations = extract_stations_river(service_root_URI, SensorThings, filter_vals, sel_vals)

# write json (data) to output file
#with open('response_stations_rivers.txt', 'w') as outfile:
#    json.dump(response_stations, outfile)

#filter_vals = 'river'

#response_river = extract_stations_river(service_root_URI, SensorThings, filter_vals, sel_vals=None)

# write json (data) to output file
#with open('response_stations_rivers.txt', 'a') as outfile:
#    json.dump(response_river, outfile)
#----------------------------------------------------


# Step 1:
# Extract details regarding the weather stations and river sections
#
# Query: https://beaware.server.de/SensorThingsService/v1.0/Things
#           ? $select=name,description,id,properties
#           & $top=1000
#           & $resultFormat=dataArray
#           & $count=true
#
# Data.table: Things = [ID, Name, Properties/Type, Description]








# Step 2:
# Extract details regarding the Datastream of each one of the Things
# Query: https://beaware.server.de/SensorThingsService/v1.0/Things?
#         $select=name,description,id,properties
#       & $expand=Datastreams($select=name,description,phenomenonTime,id)
#
# Data.table: Datastreams = [Thing_Name, Thing_ID, Dstr_Name, Dstr_ID, Dstr_description, Dstr_phenomenonTime]

# Step 3:
# For each Thing and for each datastream of it (for each row of Datastreams) do
#   find the time interval from Dstr_phenomenonTime


