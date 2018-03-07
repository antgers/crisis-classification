# Implements the 1st algorithm of Crisis Classification module
# based on the predicted water levels from AMICO for all
# river sections in the next 54h starting a specific date/time or
# the last execution of AMICO module
#
#----------------------------------------------------------------------------------------------------------
# Inputs: a) Time series of predicted water levels from AMICO for each one of the
#            river section in the next 54h starting a specific date/time or
#            for the lastRun of AMICO's program
#         b) Thresholds for each one of the river section
#
# Outputs: TOP104_METRIC_REPORT which contains the maximum predicted crisis level in the next 54h for
#           the particular river section (pre-alert visualization)
#
#   Algorithm 1 from Crisis Classification (based on AAWA)
#----------------------------------------------------------------------------------------------------------
#

from bus.bus_producer import BusProducer
from bus.CRCL_service import CRCLService
import json, time
import os, errno
from pathlib import Path
from datetime import datetime, timedelta
from math import pow, ceil

from Top104_Metric_Report import Top104_Metric_Report
from Create_Queries import extract_forecasts
from Create_Queries import extract_river_sections_loc
from Auxiliary_functions import compare_forecast_scale_thresholds, generalized_mean


# Create a directory to store the output files and TOPICS
root_path = Path.cwd()
directory = "TOPICS_Ver5"
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

#------------------------------------------------------------------------------------------------
# STEP 1: Extract the ids, the names, the properties and the location of all river sections
#
# https://beaware.server.de/SensorThingsService/v1.0/Things
# ? $filter=properties/type%20eq%20%27riverSection%27
# & $select=id,name,properties
# & $expand=Locations($select=description,location)
# & $count=true
# & $top=1000

# Start Timing Step 1
start_step1 = time.time()

SensorThings = [SensorThingEntities[0], SensorThingEntities[1]]

filt_vals = 'riverSection'
sel_vals = {'thing_sel': ['id', 'name', 'description','properties'],
            'loc_sel': ['description', 'location']}

riverSections = extract_river_sections_loc(service_root_URI, SensorThings, filt_vals, sel_vals)

# write json (data) to output file
flname = directory + "/" + 'response_riverSections.txt'
with open(flname, 'w') as outfile:
    json.dump(riverSections, outfile)

# count: number of river sections to be examined. Total river sections is 304.
count = riverSections["@iot.count"]
#count = 50

# End Timing Step 1
end_step1 = time.time()
time_duration_step1 = end_step1 - start_step1

#----------------------------------------------------------------------------------------------------
# STEP 2: Extract predicted water levels from AMICO for each one of the river sections in the next 54h
#           and find the maximum value, compares it with predefined thresholds. If this max value
#           exceeds the thresholds an appropriate message is created and sent to logger.
#----------------------------------------------------------------------------------------------------
# 2.1 Extract one measurement (forecast for water river level) from one station at specific date/time
#
# ex. Things id 390 -> River section Astico m .00
#     Date -> 2018-01-26T08:00:00.000Z

# Start Timing Step 2
start_step2 = time.time()

# Set constant variables which are utilised to create the query to extract Observations of each River Section
#
SensorThings = [SensorThingEntities[0], SensorThingEntities[1], SensorThingEntities[3], SensorThingEntities[5]]
sel_vals = {'dstr_sel': ['id', 'name', 'properties'], 'obs_sel': ['result', 'phenomenonTime', 'id', 'parameters']}
filt_args={'obs_filt': ['phenomenonTime']}
dates = ['2018-01-26T08:00:00.000Z', '2018-01-28T14:00:00.000Z']
filt_vals={'obs_filt_vals': dates}
ord_vals = ['phenomenonTime']

flag_last_run = True #False

#----------------------------------------------------------------------------------------
# Create new Producer instance using provided configuration message (dict data).

producer = BusProducer()

# Decorate terminal
print('\033[95m' + "\n***********************")
print("*** CRCL SERVICE v1.0 ***")
print("***********************\n" + '\033[0m')

# Array in which the frequency of each scale value is stored
# freqs = [N0, N1, N2, N3] where
#   N0 : number of river sections which have scale 0
#   Ni : number of river sections which have scale i, i={1,2,3}

scales = [0, 1, 2, 3]
freqs = [0]*len(scales)

for counter in range(0, count):

    print(" River Section ID = ", riverSections["value"][counter]['@iot.id'] )
    print(" and River Section Name = ", riverSections["value"][counter]['name'] )

    # Arrays to store values from the TOP104 (initialize for each river section)
    max_yValues = []
    meas_color = []
    meas_note = []
    max_measurementID = []
    max_measurementTimeStamp = []
    dataSeriesID = []
    dataSeriesName = []

    ids = {'th_id': str(riverSections["value"][counter]['@iot.id']) }

    if flag_last_run == False:
        response_forecast = extract_forecasts(service_root_URI, SensorThings, ids, sel_vals, ord_vals, filter_args=filt_args, filter_vals=filt_vals)
    else:
        response_forecast = extract_forecasts(service_root_URI, SensorThings, ids, sel_vals, ord_vals, last_run=flag_last_run)

    # write json (data) to output file
    # flname = directory + "/" + 'response_forecast_' + riverSections["value"][counter]['name'].replace(" ", "") + ".txt"
    # with open(flname, 'w') as outfile:
    #    json.dump(response_forecast, outfile)

    # Extract the thresholds of the response of riverSections query correspond to the specific river section
    #thresh = [riverSections["value"][counter]['properties']['treshold1'],
    #          riverSections["value"][counter]['properties']['treshold2'],
    #          riverSections["value"][counter]['properties']['treshold3']]

    loc_riverSection = riverSections["value"][counter]['Locations'][0]['location']['coordinates']

    # ΘΑ ΠΡΕΠΕΙ ΝΑ ΑΛΛΑΞΕΙ ΣΕ ΠΡΑΓΜΑΤΙΚΟ ΤΡΕΞΙΜΟ. ΝΑ ΔΙΑΓΡΑΦΕΙ ΧΡΗΣΗ ΠΡΑΓΜΑΤΙΚΩΝ ΚΑΤΩΦΛΙΩΝ
    thresh = [170, 180, 190]

    # Extract the observations WL forecasted values and stored in the array yValues
    Obs_yV_length = len(response_forecast['Datastreams'][0]['Observations'])

    Obs_yv = []
    for iter in range(0, Obs_yV_length):
        Obs_yv += [response_forecast['Datastreams'][0]['Observations'][iter]['result']]

    # Find all the maximum of the Obs_yv and its positions
    Obs_yv_max = max(Obs_yv)
    maxIndexList = [i for i,j in enumerate(Obs_yv) if j == Obs_yv_max]
    first_max_pos = [min(maxIndexList)]  # considers only the first maximum value

    # Calculate the Crisis Classification Level for each River Section
    #   If the maximum value exceeds one of the predefined thresholds then
    #   it stored in the topic (flag_extreme=True), otherwise it ignores (flag_extreme = False)
    #
    resp_comparison = compare_forecast_scale_thresholds(Obs_yv_max, thresh)

    flag_extreme = resp_comparison[len(resp_comparison) - 1]

    print("**** resp_comparison = ", resp_comparison)

    if flag_extreme == True and resp_comparison[0][0] != '#00FF00':
        max_yValues += [Obs_yv_max]          # for forecast
        max_yValues += [resp_comparison[2]]  # for scale

        meas_color.append( resp_comparison[0][0] )   # for forecast
        meas_color.append("")                        # for scale

        meas_note.append( resp_comparison[1][0] )    # for forecast
        meas_note.append( resp_comparison[3][0] )    # for scale

        dataSeriesID += [riverSections["value"][counter]['@iot.id']]*len(max_yValues)  # counter + 1
        dataSeriesName += [riverSections["value"][counter]['name']]*len(max_yValues)

        # Find details regarding the maximum observation and stored them in the corresponding arrays
        item = response_forecast['Datastreams'][0]['Observations'][first_max_pos[0]]
        max_measurementID += [ str(item['@iot.id']) + '_1' ] # for forecast
        max_measurementID += [ str(item['@iot.id']) + '_2' ] # for scale
        max_measurementTimeStamp += [item['phenomenonTime'].replace('.000Z', "Z")]*len(max_yValues)

        # Update the freqs array
        freqs[ resp_comparison[2] ] = freqs[ resp_comparison[2] ] + 1

        print("Update freqs = ", freqs, " at scale ", resp_comparison[2] )

        #--------------------------------------------------------------------------------------------
        #  STEP 2.2: Creates the TOPIC_104_METRIC_REPORT
        #--------------------------------------------------------------------------------------------
        #
        # Create the TOPIC 104 (json format) for the maximum value of predicted water levels
        # in the time interval defined by the 'dates' or for the lastRun of AMICO's program
        # for the specific river section.
        #
        # Set variables for the body of the message

        dataStreamGener = "CRCL"
        dataStreamName = "River Water Level Forecast"

        if flag_last_run == True:
            lastRunID = response_forecast['Datastreams'][0]["properties"]["lastRunId"]
            dataStreamID = lastRunID
            dataStreamDescript = "AMICO predictions of water level in the last run with ID:" + str(lastRunID)
        else:
            ObsRunID = response_forecast['Datastreams'][0]['Observations'][0]["parameters"]["runId"]
            dataStreamID = ObsRunID
            dataStreamDescript = "AMICO predictions of water level in the run with ID:" + str(ObsRunID) + "at dates:" + dates
        lang = "it-IT"
        dataStreamCategory = "Met"
        dataStreamSubCategory = "Flood"

        # Position of the specific river section
        position = loc_riverSection

        # Set variables for the header of the message
        district = "Vicenza"

        # Unique message identifier
        msgIdent = datetime.now().isoformat().replace(":","").replace("-","").replace(".","MS")

        sent_dateTime = datetime.now().replace(microsecond=0).isoformat() + 'Z'
        status = "Actual"
        actionType = "Update"
        scope = "Public"
        code = 20190617001

        # Call the class Top104_Metric_Report to create an object data of this class
        #
        data = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                                lang, dataStreamCategory, dataStreamSubCategory, position)

        # Record the thresholds for each river Section in the header note
        data.topic_note = "Threshold_1=" + str(thresh[0]) + ", " + "Threshold_2=" + str(thresh[1]) + ", " + "Threshold_3=" + str(thresh[2])

        # create the header of the object
        data.create_dictHeader()

        # create the measurements of the object
        #
        data.topic_yValue = max_yValues
        data.topic_measurementID = max_measurementID
        data.topic_measurementTimeStamp = max_measurementTimeStamp
        data.topic_dataSeriesID = dataSeriesID
        data.topic_dataSeriesName = dataSeriesName
        data.topic_xValue = [""]*len(max_yValues)
        data.topic_meas_color = meas_color
        data.topic_meas_note = meas_note

        # call class function
        data.create_dictMeasurements()

        # create the body of the object
        data.create_dictBody()

        # create the TOP104_METRIC_REPORT as json
        top104_forecast = {'header': data.header, 'body': data.body}

        # write json (top104_forecast) to output file
        flname = directory + "/" + 'TOP104_forecasts_' + riverSections["value"][counter]['name'].replace(" ", "") + ".txt"
        with open(flname, 'w') as outfile:
            json.dump(top104_forecast, outfile, indent=4)

        print('Send message: Max Predicted Water Level value has been forwarded to logger!')
        producer.send("TOP104_METRIC_REPORT", top104_forecast)

    else:
        # Update the freqs array
        freqs[ resp_comparison[2] ] = freqs[ resp_comparison[2] ] + 1

        print("Update N0 freqs = ", freqs, " at scale ", resp_comparison[2] )

# End Timing Step 2
end_step2 = time.time()
time_duration_step2 = end_step2 - start_step2

#--------------------------------------------------------------------------------------------
#  STEP 3: Calculates and creates the TOPIC_104_METRIC_REPORT for overall
#               Crisis Classification Index of the Region Of Interest
#--------------------------------------------------------------------------------------------
#
# Start Timing Step 3
start_step3 = time.time()

# Calculate the generalized mean
p=2
square_mean = generalized_mean(freqs, scales, p)
overall_crisis_index = ceil(square_mean)
print("\n Square mean = ", square_mean, "and overall_crisis_index = ", overall_crisis_index)

p=3
cubic_mean = generalized_mean(freqs, scales, p)
overall_crisis_index = ceil(cubic_mean)

print("Cubic mean = ", cubic_mean, "and overall_crisis_index = ", overall_crisis_index)

# Create the TOPIC 104 (json format) for the overall Crisis Classification Index
# which characterised the whole ROI

# Set variables for the body of the message

dataStreamGener = "CRCL"
dataStreamName = "Overall Crisis Classification Index"
dataStreamID = 1
dataStreamDescript = "Overall Crisis Classification Index at the Region Of Interest based on river water levels by AMICO's forecast"

lang = "it-IT"
dataStreamCategory = "Met"
dataStreamSubCategory = "Flood"

# Position of the specific river section
position = [11.54679, 45.55012]  # [longitude, latitude]

# Set variables for the header of the message
district = "Vicenza"

# Unique message identifier
msgIdent = datetime.now().isoformat().replace(":","").replace("-","").replace(".","MS")

sent_dateTime = datetime.now().replace(microsecond=0).isoformat() + 'Z'
status = "Actual"
actionType = "Update"
scope = "Public"
code = 20190617001

# Call the class Top104_Metric_Report to create an object data of this class
#
crcl_idx = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                                lang, dataStreamCategory, dataStreamSubCategory, position)

# create the header of the object
crcl_idx.create_dictHeader()

# create the measurements of the object
#
crcl_idx.topic_yValue = [overall_crisis_index]
if overall_crisis_index == 1:
    crcl_idx.topic_meas_color = ['#FFFF00']  # yellow
    crcl_idx.topic_meas_note = ['Overall Crisis Classification Index is LOW!!!']
elif overall_crisis_index == 2:
    crcl_idx.topic_meas_color = ['#FFA500']  # orange
    crcl_idx.topic_meas_note = ['Overall Crisis Classification Index is MEDIUM!!!']
elif overall_crisis_index == 3:
    crcl_idx.topic_meas_color = ['#FF0000'] # red
    crcl_idx.topic_meas_note = ['Overall Crisis Classification Index is HIGH!!!']
else:
    crcl_idx.topic_meas_color = ['#00FF00']  # green
    crcl_idx.topic_meas_note = ['Overall Crisis Classification Index is TRIVIAL!!!']

crcl_idx.topic_measurementID = ["1"]
crcl_idx.topic_measurementTimeStamp = [sent_dateTime]
crcl_idx.topic_dataSeriesID = ["1"]
crcl_idx.topic_dataSeriesName = ["Overall Crisis Classification Index"]
crcl_idx.topic_xValue = [""]

# call class function
crcl_idx.create_dictMeasurements()

# create the body of the object
crcl_idx.create_dictBody()

# create the TOP104_METRIC_REPORT as json
top104_crcl_idx = {'header': crcl_idx.header, 'body': crcl_idx.body}

# write json (top104_forecast) to output file
flname = directory + "/" + 'TOP104_Overall_CRCL_Index.txt'
with open(flname, 'w') as outfile:
    json.dump(top104_crcl_idx, outfile, indent=4)

print('Send message: Overall Crisis Classification Index has been forwarded to logger!')
producer.send("TOP104_METRIC_REPORT", top104_crcl_idx)


# End Timing Step 3
end_step3 = time.time()
time_duration_step3 = end_step3 - start_step3


total_time = time_duration_step1 + time_duration_step2 + time_duration_step3

print("\n ****** EXECUTION TIME: **** ")
print(" Time for Step 1: ", time_duration_step1, " seconds")
print(" Time for Step 2: ", time_duration_step2, " seconds")
print(" Time for Step 3: ", time_duration_step3, " seconds")
print(" Total Execution Time: ", total_time/60.0, " minutes")
print(" ************************** \n")




# topics = ['TOP104_METRIC_REPORT']
# crcl_service = CRCLService(listen_to_topics=topics)
# crcl_service.run_service()

