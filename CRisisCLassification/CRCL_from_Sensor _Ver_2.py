# Version 2
# Created: 22/02/2018
# Modified: 23/02/2018
#
# Implements the 2nd algorithm of Crisis Classification module
# based on the measurements of water levels and rainfall intensity
# from sensors for a specific weather stations at a) last measurement
# or b) at particular date/time period (not supported)
#
#----------------------------------------------------------------------------------------------------------
# Inputs: a) Time series of measurements of water levels by the sensors
#               for a specific weather station at last measurement (date/time)
#         b) Time series of metereological measurements such as rainfall intensity / precipitation,
#               for the specific weather station
#         c) Thresholds for the particular specific weather station and for every raingages
#
# Outputs: TOP104_METRIC_REPORT for each Weather Station and Datastream, which contains:
#         a) the actual crisis level associated to the sensor position
#         b) a metric-scale from 0 to 3 depending on whether the actual value exceeds a particular alarm
#            threshold.
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
from Auxiliary_functions import compare_value_scale_thresholds

# Create a directory to store the output files and TOPICS
root_path = Path.cwd()
directory = "TOPICS_fromSensors_VER_2"
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

    SensThings = [SensorThingEntities[0], SensorThingEntities[3]]
    selVals = {'dstr_sel': ['id', 'name', 'phenomenonTime']}
    filt_args={'thing_filt': ['id'], 'dstr_filt': ['name']}
    filt_vals_WL={'thing_filt': str(StationID), 'dstr_filt': ['Water']}
    filt_vals_PR={'thing_filt': str(StationID), 'dstr_filt': ['Precipitation']}

    resp_station_datastream_WL = extract_station_datastream(service_root_URI, SensThings, selVals, filt_args, filt_vals_WL)

    resp_station_datastream_PR = extract_station_datastream(service_root_URI, SensThings, selVals, filt_args, filt_vals_PR)

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

    filt_args={'thing_filt': ['id'], 'dstr_filt': ['name'], 'obs_filt': ['phenomenonTime']}
    sel_vals = {'thing_sel': ['id','name', 'description'],
                'dstr_sel': ['id', 'name', 'phenomenonTime'],
                'obs_sel': ['result', 'phenomenonTime', 'id']}
    ord_vals = ['phenomenonTime']

    # For WL datastream do:
    if StationID['WL'] != None:

        # Find the corresponding PhenomenonTimeDate for WL of the Station
        for k, j in enumerate(dates_WL):
            if j['ID'] == StationID['ID']:
                if len(j) > 1:   #['PhenDateTime'] != None:
                    dt = j['PhenDateTime']
                    filt_vals_WL={'thing_filt': [str(StationID['ID'])], 'dstr_filt': ['Water'], 'obs_filt_vals': [dt]}

        # Call function to extract the measurement of WL from specific Station
        item_WL = extract_from_WS_Sensors(service_root_URI, SensorThings, sel_vals, ord_vals, filt_args, filt_vals_WL)
        response_sensors_WL.append(item_WL)

        msg_WL = "\n Station ID = " + str(StationID['ID']) + " and Datastream ID = " + str(StationID['WL']) + "\n"
        outfl_WL.write(msg_WL)
        json.dump(item_WL, outfl_WL)
        outfl_WL.write("\n ------------------------------ \n")

        value = item_WL['value'][0]['Datastreams'][0]['Observations'][0]['result']

        # call function to compare the value with alarm thresholds
        if value != 0.0 or value == 0.0:
            color_note_WL = compare_value_scale_thresholds(value, filt_vals_WL['thing_filt'], filt_vals_WL['dstr_filt'], Thresholds_WL)
            meas_ColNote_WL_dict = {'ID': StationID['ID'], 'col': color_note_WL[0], 'note': color_note_WL[1],
                                    'scale': color_note_WL[2], 'note_scale': color_note_WL[3]}
    else: # StationID['WL'] == None:
         meas_ColNote_WL_dict = {'ID': StationID['ID'], 'col': None, 'note': None, 'scale': None, 'note_scale': None}

    meas_ColNote_WL += [meas_ColNote_WL_dict]

    # For Precipitation datastream do:
    if StationID['PR'] != None:

        # Find the corresponding PhenomenonTimeDate for PR of the Station
        for k, j in enumerate(dates_PR):
            if j['ID'] == StationID['ID']:
                if len(j) > 1:    #['PhenDateTime'] != None:
                    dt = j['PhenDateTime']
                    filt_vals_PR={'thing_filt': [str(StationID['ID'])], 'dstr_filt': ['Precipitation'], 'obs_filt_vals': [dt]}

        # Call function to extract the measurement of PR from specific Station
        item_PR = extract_from_WS_Sensors(service_root_URI, SensorThings, sel_vals, ord_vals, filt_args, filt_vals_PR)
        response_sensors_PR.append(item_PR)

        msg_PR = "\n Station ID = " + str(StationID['ID']) + " and Datastream ID = " + str(StationID['PR']) + "\n"
        outfl_PR.write(msg_PR)
        json.dump(item_PR, outfl_PR)
        outfl_PR.write("\n ------------------------------ \n")

        value = item_PR['value'][0]['Datastreams'][0]['Observations'][0]['result']

        # call function to compare the value with alarm thresholds
        if value != 0.0 or value == 0.0:

            color_note_PR = compare_value_scale_thresholds(value, filt_vals_PR['thing_filt'], filt_vals_PR['dstr_filt'], Thresholds_PR)
            meas_ColNote_PR_dict ={'ID': StationID['ID'], 'col': color_note_PR[0], 'note': color_note_PR[1],
                                   'scale': color_note_PR[2], 'note_scale': color_note_PR[3]}
    else: # StationID['PR'] == None:
        meas_ColNote_PR_dict = {'ID': StationID['ID'], 'col': None, 'note': None, 'scale': None, 'note_scale': None}

    meas_ColNote_PR += [meas_ColNote_PR_dict]


# Close files
outfl_WL.close()
outfl_PR.close()

#--------------------------------------------------------------------------------------------
#  STEP 3: Creates the TOPIC_104_METRIC_REPORT
#--------------------------------------------------------------------------------------------
#
# Create the TOPIC 104 (json format) for each Weather Station and each Datastream
# (Water Level and Precipitation). Each datastream will be consisted of real values
# retrieved from the sensors at a particular Weather Station and another dataSeries
# metric which presents the scale.
#
#----------------------------------------------------------------------------------------
# Create new Producer instance using provided configuration message (dict data).
#
producer = BusProducer()

# Decorate terminal
print('\033[95m' + "\n***********************")
print("*** CRCL SERVICE v1.0 ***")
print("***********************\n" + '\033[0m')

#-----------------------------------------------

# for each Weather Station
for iter, item_WS in enumerate(WSDS):

    # List to store all the Topics 104
    Topics104 = []

    # Set variables for the header of the message
    district = "Vicenza"
    #msgIdent = "5433dfde68"

    sent_dateTime = datetime.now().replace(microsecond=0).isoformat() + 'Z'
    status = "Actual"
    actionType = "Update"
    scope = "Public"
    code = 20190617001

    # Set variables for the body of the message
    dataStreamGener = "CRCL"
    lang = "it-IT"
    dataStreamCategory = "Met"
    dataStreamSubCategory = "Flood"

    # Position of the Weather Station
    position = [ item_WS['Coordinates'][0], item_WS['Coordinates'][1] ]

    #-------------------------------------------------------------------------
    # If the Water Level datastream exist in the specific Weather Station

    # Initialize temporary arrays
    measurement_ID = []
    measurement_TimeStamp = []
    dataSeriesID = []
    dataSeriesName = []
    dsmeas_color = []
    dsmeas_note = []
    yValues = []

    if item_WS['WL'] != None:

        dataStreamName = item_WS['WL_name']
        dataStreamDescript = 'Real measurements' + dataStreamName
        dataStreamID = item_WS['WL']

        # Unique message identifier
        msgIdent = datetime.now().isoformat().replace(":","").replace("-","").replace(".","MS")

        # Call the class Top104_Metric_Report to create an object data of this class
        data_WL = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                            dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                            lang, dataStreamCategory, dataStreamSubCategory, position)

        # Create the header of the object (message)
        data_WL.create_dictHeader()

        # Create the body and the measurements of the object (message)
        #
        # Extract values from 'response_sensors_WL'
        pos = [i for i, x in enumerate(response_sensors_WL) if x['value'][0]['@iot.id'] == item_WS['ID'] ]
        measurement_ID += [ response_sensors_WL[pos[0]]['value'][0]['Datastreams'][0]['Observations'][0]['@iot.id'] ]
        measurement_TimeStamp += [ response_sensors_WL[pos[0]]['value'][0]['Datastreams'][0]['Observations'][0]['phenomenonTime'] ]

        # find the position of station and datasteam to the meas_ColNote_WL list
        pos_meas = [i for i, x in enumerate(meas_ColNote_WL) if x['ID'] == item_WS['ID'] ]

        # First measurement - real values
        dataSeriesID += ["1"]
        dataSeriesName += ['Real measurement of ' + dataStreamName]

        dsmeas_color += [ meas_ColNote_WL[pos_meas[0]]['col'][0] ]
        dsmeas_note  += [ meas_ColNote_WL[pos_meas[0]]['note'][0] ]
        yValues += [ item_WL['value'][0]['Datastreams'][0]['Observations'][0]['result'] ]

        # Second measurement for the scale
        measurement_ID += [ response_sensors_WL[pos[0]]['value'][0]['Datastreams'][0]['Observations'][0]['@iot.id'] ]
        measurement_TimeStamp += [ response_sensors_WL[pos[0]]['value'][0]['Datastreams'][0]['Observations'][0]['phenomenonTime'] ]

        dataSeriesID += ["2"]
        dataSeriesName += ['Scale']
        yValues += [ meas_ColNote_WL[pos_meas[0]]['scale'] ]
        dsmeas_color += [""]
        dsmeas_note += [ meas_ColNote_WL[pos_meas[0]]['note_scale'][0] ]

        # Set values to the data_WL attributes from temporary arrays
        data_WL.topic_measurementID = measurement_ID
        data_WL.topic_measurementTimeStamp = measurement_TimeStamp
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

        # write json (top104_WL) to output file
        flname = directory + "/" + 'TOP104_Water_Level_' + 'WeatherStation_' + str(item_WS['ID']) + '.txt'
        with open(flname, 'w') as outfile:
            json.dump(top104_WL, outfile, indent=4)

        Topics104 += [ top104_WL ]

    #-----------------------------------------------------------------------------
    # If the Precipitation datastream exist in the specific Weather Station
    #
    # Initialize temporary arrays
    measurement_ID = []
    measurement_TimeStamp = []
    dataSeriesID = []
    dataSeriesName = []
    dsmeas_color = []
    dsmeas_note = []
    yValues = []

    if item_WS['PR'] != None:

        dataStreamName = item_WS['PR_name']
        dataStreamDescript = 'Real measurements' + dataStreamName
        dataStreamID = item_WS['PR']

        # Unique message identifier
        msgIdent = datetime.now().isoformat().replace(":","").replace("-","").replace(".","MS")

        # Call the class Top104_Metric_Report to create an object data of this class
        data_PR = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                            dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                            lang, dataStreamCategory, dataStreamSubCategory, position)

        # Create the header of the object (message)
        data_PR.create_dictHeader()

        # Create the body and the measurements of the object (message)
        #
        # Extract values from 'response_sensors_PR'
        pos = [i for i, x in enumerate(response_sensors_PR) if x['value'][0]['@iot.id'] == item_WS['ID'] ]
        measurement_ID += [ response_sensors_PR[pos[0]]['value'][0]['Datastreams'][0]['Observations'][0]['@iot.id'] ]
        measurement_TimeStamp += [ response_sensors_PR[pos[0]]['value'][0]['Datastreams'][0]['Observations'][0]['phenomenonTime'] ]

        # find the position of station and datasteam to the meas_ColNote_PR list
        pos_meas = [i for i, x in enumerate(meas_ColNote_PR) if x['ID'] == item_WS['ID'] ]

        # First measurement - real values
        dataSeriesID += ["1"]
        dataSeriesName += ['Real measurement of ' + dataStreamName]

        dsmeas_color += [ meas_ColNote_PR[pos_meas[0]]['col'][0] ]
        dsmeas_note  += [ meas_ColNote_PR[pos_meas[0]]['note'][0] ]
        yValues += [ item_PR['value'][0]['Datastreams'][0]['Observations'][0]['result'] ]

        # Second measurement for the scale
        measurement_ID += [ response_sensors_PR[pos[0]]['value'][0]['Datastreams'][0]['Observations'][0]['@iot.id'] ]
        measurement_TimeStamp += [ response_sensors_PR[pos[0]]['value'][0]['Datastreams'][0]['Observations'][0]['phenomenonTime'] ]

        dataSeriesID += ["2"]
        dataSeriesName += ['Scale']
        yValues += [ meas_ColNote_PR[pos_meas[0]]['scale'] ]
        dsmeas_color += [""]
        dsmeas_note += [ meas_ColNote_PR[pos_meas[0]]['note_scale'][0] ]

        # Set values to the data_WL attributes from temporary arrays
        data_PR.topic_measurementID = measurement_ID
        data_PR.topic_measurementTimeStamp = measurement_TimeStamp
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

        # write json (top104_PR) to output file
        flname = directory + "/" + 'TOP104_Precipitation_' + 'WeatherStation_' + str(item_WS['ID']) + '.txt'
        with open(flname, 'w') as outfile:
            json.dump(top104_PR, outfile, indent=4)

        Topics104 += [ top104_PR ]

    # Send messages for the specific Weather Station
    if len(Topics104) != 0:
        print('Messages from Weather Station', str(item_WS['ID']), 'have been forwarded to logger!')
        for i in range(len(Topics104)):
            producer.send("TOP104_METRIC_REPORT", Topics104[i])
    else:
        print('No messages will be forward to logger from Weather Station', str(item_WS['ID']), "!" )
