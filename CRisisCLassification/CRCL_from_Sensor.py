# Implements the 2nd algorithm of Crisis Classification module
# based on the measurements of water levels and rainfall intensity
# from sensors for a specific weather stations at a) last measurement
# or b) at particular date/time period (not supported)

#----------------------------------------------------------------------------------------------------------
# Inputs: a) Time series of measurements of water levels by the sensors
#               for a specific weather station at last measurement (date/time)
#         b) Time series of metereological measurements such as rainfall intensity / precipitation,
#               for the specific weather station
#         c) Thresholds for the particular specific weather station and for every raingages
#
# Outputs: TOP104_METRIC_REPORT which contains the actual crisis level associated to the sensor position
#
#   Algorithm 2 from Crisis Classification (based on AAWA)
#----------------------------------------------------------------------------------------------------------
#

from bus.bus_producer import BusProducer
from bus.CRCL_service import CRCLService
import json, time
from datetime import datetime, timedelta
import os, errno
from pathlib import Path

from Top104_Metric_Report import Top104_Metric_Report
from Create_Queries import extract_from_WS_Sensors, extract_stations_river, \
    extract_forecasts, extract_station_datastream, extract_station_location
from Auxiliary_functions import compare_value_thresholds

# Create a directory to store the output files and TOPICS
root_path = Path.cwd()
directory = "TOPICS_fromSensors"
os.makedirs(directory, exist_ok=True)

#-----------------------------------------------------------------------------------
# Fetch data from the OGC SensorThings API
#
# User defined values in order to formulate the query
#
service_root_URI = 'https://beaware.server.de/SensorThingsService/v1.0/'

SensorThingEntities = ['Things', 'Locations', 'HistoricalLocations',
                        'Datastreams', 'Sensor', 'Observations',
                        'ObservedProperties', 'FeaturesOfInterest', 'MultiDatastreams']

SensorThings = [SensorThingEntities[0], SensorThingEntities[3], SensorThingEntities[5]]

# Initialise arrays to store the results of comparison for each weather station and each datastream (WL or PR)
meas_ColNote_WL = []
meas_ColNote_PR = []

#--------------------------------------------------------------------------------------
# Creates the thresholds for each one of the Weather Stations of interest
#
Weather_Stations_Ids = [45, 47, 49, 374]

Thresholds_WL = [{'ID': 45, 'Alarm1': 4.36, 'Alarm2': 4.86, 'Alarm3': 5.66},
                 {'ID': 47, 'Alarm1': 3.00, 'Alarm2': 4.60, 'Alarm3': 5.40},
                 {'ID': 374, 'Alarm1': 1.63, 'Alarm2': 3.03, 'Alarm3': 3.43}
                ]
Thresholds_PR = [{'ID': 47, 'Alarm1': 50, 'Alarm2': 100, 'Alarm3': 150},
                 {'ID': 49, 'Alarm1': 50, 'Alarm2': 100, 'Alarm3': 150},
                 {'ID': 374, 'Alarm1': 50, 'Alarm2': 100, 'Alarm3': 150}
                ]
#-----------------------------------------------------------------------------------------------------
# Step 1: Extracts the weather stations where have as Datastreams the Water Level or/and Precipitation

flag_last_measurement = True  # or False

# List of dictionaries contains the id of each WS and its one of the Datastreams. For WS where one of the
# Datastreams is missing the None value is filled
WSDS = []

dates_WL=[]
dates_PR=[]

for i, StationID in enumerate(Weather_Stations_Ids):

    WSDS_dict = {'ID': StationID}
    dates_WL_dict = {'ID': StationID}
    dates_PR_dict = {'ID': StationID}

    # extract the location of the station
    SensThings_Loc = [SensorThingEntities[0], SensorThingEntities[1]]
    selVals = {'thing_sel': ['id', 'name'], 'loc_sel': ['location']}
    filt_args = {'thing_filt': ['id']}
    filt_vals = {'thing_filt': str(StationID)}

    resp_station_loc = extract_station_location(service_root_URI, SensThings_Loc, selVals, filt_args, filt_vals)
#    print("\n======")
#    print(resp_station_loc)

    SensThings = [SensorThingEntities[0], SensorThingEntities[3]]
    selVals = {'dstr_sel': ['id', 'name', 'phenomenonTime']}
    filt_args={'thing_filt': ['id'], 'dstr_filt': ['name']}
    filt_vals_WL={'thing_filt': str(StationID), 'dstr_filt': ['Water']}
    filt_vals_PR={'thing_filt': str(StationID), 'dstr_filt': ['Precipitation']}

    resp_station_datastream_WL = extract_station_datastream(service_root_URI, SensThings, selVals, filt_args, filt_vals_WL)
    #print("\n======")
    #print(resp_station_datastream_WL)

    resp_station_datastream_PR = extract_station_datastream(service_root_URI, SensThings, selVals, filt_args, filt_vals_PR)
    #print("\n======")
    #print(resp_station_datastream_PR)

    # Update WSDS with Weather Station name
    WSDS_dict.update({'WS_name': resp_station_datastream_WL['value'][0]['name']})

    # Keep elements and values for Water Level
    if len(resp_station_datastream_WL['value'][0]['Datastreams']) == 0:
         WSDS_dict.update({'WL': None})
         WSDS_dict.update({'WL_name': None})
    else:
         WSDS_dict.update({'WL': resp_station_datastream_WL['value'][0]['Datastreams'][0]['@iot.id']})
         WSDS_dict.update({'WL_name': resp_station_datastream_WL['value'][0]['Datastreams'][0]['name']})
         PhenDateTime = resp_station_datastream_WL['value'][0]['Datastreams'][0]['phenomenonTime']
         dates_WL_dict.update({'PhenDateTime': PhenDateTime[(PhenDateTime.find("/")+1):] })

    # Keep elements and values for Precipitation
    if len(resp_station_datastream_PR['value'][0]['Datastreams']) == 0:
         WSDS_dict.update({'PR': None})
         WSDS_dict.update({'PR_name': None})
    else:
        WSDS_dict.update({'PR': resp_station_datastream_PR['value'][0]['Datastreams'][0]['@iot.id']})
        WSDS_dict.update({'PR_name': resp_station_datastream_PR['value'][0]['Datastreams'][0]['name']})
        PhenDateTime = resp_station_datastream_PR['value'][0]['Datastreams'][0]['phenomenonTime']
        dates_PR_dict.update({'PhenDateTime': PhenDateTime[(PhenDateTime.find("/")+1):] })

    # Add station's location to WSDS_dict
    WSDS_dict.update({'Coordinates': resp_station_loc['value'][0]['Locations'][0]['location']['coordinates']})

    # Update the WSDS with the new dictionary for the WS
    WSDS += [ WSDS_dict ]
    dates_WL += [ dates_WL_dict ]
    dates_PR += [ dates_PR_dict ]

print("\n=======================")
print('WSDS = ',WSDS)
print("=======================\n")

#print('\n dates_PR=', dates_PR)
#print('\n dates_WL=', dates_WL)

#-----------------------------------------------------------------------------------
# Step 2: Extract real measurements from Sensors at the specific Weather Station
#

# Open files to store the query responses
flname_WL = directory + "/" + 'response_Sensors_WL.txt'
outfl_WL = open(flname_WL, 'w')

flname_PR = directory + "/" + 'response_Sensors_PR.txt'
outfl_PR = open(flname_PR, 'w')

# Arrays to keep the query responses
response_sensors_WL = []
response_sensors_PR = []

for i, StationID in enumerate(WSDS):

#    print("\n ====>>>> NEW STATION: ", i, ",", StationID['ID'], ",",StationID['WL'], ",",StationID['PR'] )

    filt_args={'thing_filt': ['id'], 'dstr_filt': ['name'], 'obs_filt': ['phenomenonTime']}
    sel_vals = {'thing_sel': ['id','name', 'description'],
                'dstr_sel': ['id', 'name', 'phenomenonTime'],
                'obs_sel': ['result', 'phenomenonTime']}
    ord_vals = ['phenomenonTime']

    # For WL datastream do:
    if StationID['WL'] != None:

        # Find the corresponding PhenomenonTimeDate for WL of the Station
        for k, j in enumerate(dates_WL):
            if j['ID'] == StationID['ID']:
                if len(j) > 1:   #['PhenDateTime'] != None:
                    dt = j['PhenDateTime']
                    filt_vals_WL={'thing_filt': [str(StationID['ID'])], 'dstr_filt': ['Water'], 'obs_filt_vals': [dt]}
            #        print("\n ID= ", j['ID'], ",", "filt_vals_WL = ", filt_vals_WL, " in STATION=", StationID['ID'])
          #  else:
            #    print("WL Different from StationID", StationID['ID'], "from j[ID]=",  j['ID'])

        # Call function to extract the measurement of WL from specific Station
        item_WL = extract_from_WS_Sensors(service_root_URI, SensorThings, sel_vals, ord_vals, filt_args, filt_vals_WL)
        response_sensors_WL.append(item_WL)

     #   print("\n Write response to file! ")
     #   print("Length = ", len(response_sensors_WL))
     #   print(response_sensors_WL[len(response_sensors_WL)-1])

        msg_WL = "\n Station ID = " + str(StationID['ID']) + " and Datastream ID = " + str(StationID['WL']) + "\n"
        outfl_WL.write(msg_WL)
        json.dump(item_WL, outfl_WL)
        outfl_WL.write("\n ------------------------------ \n")

        value = item_WL['value'][0]['Datastreams'][0]['Observations'][0]['result']

        # call function to compare the value with alarm thresholds
        if value != 0.0 or value == 0.0:
            color_note_WL = compare_value_thresholds(value, filt_vals_WL['thing_filt'], filt_vals_WL['dstr_filt'], Thresholds_WL)
            meas_ColNote_WL_dict = {'ID': StationID['ID'], 'col': color_note_WL[0], 'note': color_note_WL[1]}
    else: # StationID['WL'] == None:
         meas_ColNote_WL_dict = {'ID': StationID['ID'], 'col': None, 'note': None}

    meas_ColNote_WL += [meas_ColNote_WL_dict]

    # For Precipitation datastream do:
    if StationID['PR'] != None:

        # Find the corresponding PhenomenonTimeDate for PR of the Station
        for k, j in enumerate(dates_PR):
            if j['ID'] == StationID['ID']:
                if len(j) > 1:    #['PhenDateTime'] != None:
                    dt = j['PhenDateTime']
                    filt_vals_PR={'thing_filt': [str(StationID['ID'])], 'dstr_filt': ['Precipitation'], 'obs_filt_vals': [dt]}
        #            print("\n ID= ", j['ID'], ",", "filt_vals_PR = ", filt_vals_PR, " in STATION=", StationID['ID'])
        #    else:
        #        print("PR Different from StationID", StationID['ID'], "from j[ID]=",  j['ID'])

        # Call function to extract the measurement of PR from specific Station
        item_PR = extract_from_WS_Sensors(service_root_URI, SensorThings, sel_vals, ord_vals, filt_args, filt_vals_PR)
        response_sensors_PR.append(item_PR)

    #    print("\n Write response to file! ")
    #    print("Length = ", len(response_sensors_PR))
    #    print(response_sensors_PR[len(response_sensors_PR)-1])

        msg_PR = "\n Station ID = " + str(StationID['ID']) + " and Datastream ID = " + str(StationID['PR']) + "\n"
        outfl_PR.write(msg_PR)
        json.dump(item_PR, outfl_PR)
        outfl_PR.write("\n ------------------------------ \n")

        value = item_PR['value'][0]['Datastreams'][0]['Observations'][0]['result']

        # call function to compare the value with alarm thresholds
        if value != 0.0 or value == 0.0:

            color_note_PR = compare_value_thresholds(value, filt_vals_PR['thing_filt'], filt_vals_PR['dstr_filt'], Thresholds_PR)
            meas_ColNote_PR_dict ={'ID': StationID['ID'], 'col': color_note_PR[0], 'note': color_note_PR[1]}
    else: # StationID['PR'] == None:
        meas_ColNote_PR_dict = {'ID': StationID['ID'], 'col': None, 'note': None}

    meas_ColNote_PR += [meas_ColNote_PR_dict]

# print("\n=================")
# print("Precipitation:")
# print(meas_ColNote_PR)

# print("\n=================")
# print("Water Level:")
# print(meas_ColNote_WL)


# Close files
outfl_WL.close()
outfl_PR.close()

#--------------------------------------------------------------------------------------------
#  STEP 3: Creates the TOPIC_104_METRIC_REPORT
#--------------------------------------------------------------------------------------------
#
# Create the TOPIC 104 (json format) for each Datastream (Water Level and Precipitation)
# of real values retrieved from the sensors at specific Weather Stations.
#
#-----------------------------------------------
# STEP 3.A: TOP104 FOR WATER LEVEL

# Set variables for the header of the message
district = "Vicenza"
#msgIdent = "5433dfde68"
# Unique message identifier
msgIdent = datetime.now().isoformat().replace(":","").replace("-","").replace(".","MS")

sent_dateTime = datetime.now().replace(microsecond=0).isoformat() + 'Z'
status = "Actual"
actionType = "Update"
scope = "Public"
code = 20190617001

# Set variables for the body of the message
dataStreamGener = "CRCL"
dataStreamName = 'Water Level'
dataStreamDescript = 'Real measurements of Water Level at Weather Stations'
dataStreamID = '1'
lang = "it-IT"
dataStreamCategory = "Met"
dataStreamSubCategory = "Flood"

# Arbitrary position in the center of Vicenca (longtitude, latitude)
position = ["11.53542","45.54547"]

# Call the class Top104_Metric_Report to create an object data of this class
#
data_WL = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                            dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                            lang, dataStreamCategory, dataStreamSubCategory, position)

# create the header of the object
data_WL.create_dictHeader()

# Create the measurements of the body -- The WL value for each station is a different dataSeries

# Temporary arrays to store values
yValues = []
dsmeas_color = []
dsmeas_note = []
dstr_measurementID = []
dstr_measurementTimeStamp = []
dataSeriesID = []
dataSeriesName = []

for i, item_WL in enumerate(response_sensors_WL):

   # Station and Datastream ID for each response_sensors_WL line
   st_id = item_WL['value'][0]["@iot.id"]
   dstr_id = item_WL['value'][0]['Datastreams'][0]["@iot.id"]

   # find the position of station and datasteam to the meas_ColNote_WL list
   pos = [i for i, x in enumerate(meas_ColNote_WL) if x['ID'] == st_id]

   dataSeriesID += [st_id]
   dataSeriesName += [item_WL['value'][0]['name']]
   dstr_measurementID += [dstr_id]
   dstr_measurementTimeStamp += [item_WL['value'][0]['Datastreams'][0]['Observations'][0]['phenomenonTime'].replace('.000Z', "Z")]
   dsmeas_color += meas_ColNote_WL[pos[0]]['col']
   dsmeas_note += meas_ColNote_WL[pos[0]]['note']
   yValues += [item_WL['value'][0]['Datastreams'][0]['Observations'][0]['result']]

# Set values to the data_WL attributes from temporary arrays
data_WL.topic_measurementID = dstr_measurementID
data_WL.topic_measurementTimeStamp = dstr_measurementTimeStamp
data_WL.topic_dataSeriesID = dataSeriesID
data_WL.topic_dataSeriesName = dataSeriesName
data_WL.topic_xValue = [""]*len(yValues)
data_WL.topic_yValue = yValues
data_WL.topic_meas_color = dsmeas_color
data_WL.topic_meas_note = dsmeas_note

# call class function
data_WL.create_dictMeasurements()

# create the body of the object
data_WL.create_dictBody()

# create the TOP104_METRIC_REPORT as json
top104_WL = {'header': data_WL.header, 'body': data_WL.body}

# write json (top104_forecast) to output file
flname = directory + "/" + 'TOP104_Water_Level.txt'
with open(flname, 'w') as outfile:
    json.dump(top104_WL, outfile, indent=4)

#-----------------------------------------------
# STEP 3.B: TOP104 FOR PRECIPITATION

# Set variables for the header of the message
district = "Vicenza"
#msgIdent = "5433dfde68"
# Unique message identifier
msgIdent = datetime.now().isoformat().replace(":","").replace("-","").replace(".","MS")

sent_dateTime = datetime.now().replace(microsecond=0).isoformat() + 'Z'
status = "Actual"
actionType = "Update"
scope = "Public"
code = 20190617001

# Set variables for the body of the message
dataStreamGener = "CRCL"
dataStreamName = 'Precipitation'
dataStreamDescript = 'Real measurements of Precipitation at Weather Stations'
dataStreamID = '2'
lang = "it-IT"
dataStreamCategory = "Met"
dataStreamSubCategory = "Flood"

# Arbitrary position in the center of Vicenca
position = ["11.55", "45.54"]

# Call the class Top104_Metric_Report to create an object data of this class
#
data_PR = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                            dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                            lang, dataStreamCategory, dataStreamSubCategory, position)

# create the header of the object
data_PR.create_dictHeader()

# Create the measurements of the body -- The PR value for each station is a different dataSeries

# Temporary arrays to store values
yValues = []
dsmeas_color = []
dsmeas_note = []
dstr_measurementID = []
dstr_measurementTimeStamp = []
dataSeriesID = []
dataSeriesName = []

for i, item_PR in enumerate(response_sensors_PR):

   # Station and Datastream ID for each response_sensors_PR line
   st_id = item_PR['value'][0]["@iot.id"]
   dstr_id = item_PR['value'][0]['Datastreams'][0]["@iot.id"]

   # find the position of station and datasteam to the meas_ColNote_PR list
   pos = [i for i, x in enumerate(meas_ColNote_PR) if x['ID'] == st_id]

   dataSeriesID += [st_id]
   dataSeriesName += [item_PR['value'][0]['name']]
   dstr_measurementID += [dstr_id]
   dstr_measurementTimeStamp += [item_PR['value'][0]['Datastreams'][0]['Observations'][0]['phenomenonTime'].replace('.000Z', "Z")]
   dsmeas_color += [meas_ColNote_PR[pos[0]]['col']]
   dsmeas_note += [meas_ColNote_PR[pos[0]]['note']]
   yValues += [item_PR['value'][0]['Datastreams'][0]['Observations'][0]['result']]

# Set values to the data_PR attributes from temporary arrays
data_PR.topic_measurementID = dstr_measurementID
data_PR.topic_measurementTimeStamp = dstr_measurementTimeStamp
data_PR.topic_dataSeriesID = dataSeriesID
data_PR.topic_dataSeriesName = dataSeriesName
data_PR.topic_xValue = [""]*len(yValues)
data_PR.topic_yValue = yValues
data_PR.topic_meas_color = dsmeas_color
data_PR.topic_meas_note = dsmeas_note

# call class function
data_PR.create_dictMeasurements()

# create the body of the object
data_PR.create_dictBody()

# create the TOP104_METRIC_REPORT as json
top104_PR = {'header': data_PR.header, 'body': data_PR.body}

# write json (top104_forecast) to output file
flname = directory + "/" + 'TOP104_Precipitation.txt'
with open(flname, 'w') as outfile:
    json.dump(top104_PR, outfile, indent=4)


#----------------------------------------------------------------------------------------
# Create new Producer instance using provided configuration message (dict data).

#producer = BusProducer()

# Decorate terminal
print('\033[95m' + "\n***********************")
print("*** CRCL SERVICE v1.0 ***")
print("***********************\n" + '\033[0m')

print('First message: Water Level values from Weather Stations have been forwarded to logger!')
#producer.send("TOP104_METRIC_REPORT", top104_WL)

print('Second message: Precipitation from Weather Stations have been forwarded to logger!')
#producer.send("TOP104_METRIC_REPORT", top104_PR)

#topics = ['TOP104_METRIC_REPORT', 'TOP104_METRIC_REPORT']
#crcl_service = CRCLService(listen_to_topics=topics)
#crcl_service.run_service()

# Sleep for a while and then stop the service
#time.sleep(0.72)
#crcl_service.stop_service()