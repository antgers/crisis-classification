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

from Top104_Metric_Report import Top104_Metric_Report
from Create_Queries import extract_forecasts
from Create_Queries import extract_stations_river

# Create a directory to store the output files and TOPICS
root_path = Path.cwd()
directory = "TOPICS_Ver4"
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
# STEP 1: Extract the ids, the names and the properties of all the river sections
#
# https://beaware.server.de/SensorThingsService/v1.0/Things
# ? $filter=properties/type%20eq%20%27riverSection%27
# & $select=id,name,properties
# & $count=true
# & $top=1000

# Start Timing Step 1
start_step1 = time.time()

SensorThings = [SensorThingEntities[0]]

filt_vals = 'riverSection'
sel_vals = ['id', 'name', 'description', 'properties']

riverSections = extract_stations_river(service_root_URI, SensorThings, filt_vals, sel_vals)

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

#------------------------------------------------------------------------------------------------
# STEP 2: Extract predicted water levels from AMICO for a specific river section in the next 54h
#------------------------------------------------------------------------------------------------
# Extract one measurement (forecast for water river level) from one station at specific date/time
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

# Arrays to store values from the TOP104
max_yValues = []
meas_color = []
meas_note = []
max_measurementID = []
max_measurementTimeStamp = []
dataSeriesID = []
dataSeriesName = []


for counter in range(0, count):

    print(" River Section ID = ", riverSections["value"][counter]['@iot.id'] )
    print(" and River Section Name = ", riverSections["value"][counter]['name'] )

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

    # ΘΑ ΠΡΕΠΕΙ ΝΑ ΑΛΛΑΞΕΙ ΣΕ ΠΡΑΓΜΑΤΙΚΟ ΤΡΕΞΙΜΟ. ΝΑ ΔΙΑΓΡΑΦΕΙ ΧΡΗΣΗ ΠΡΑΓΜΑΤΙΚΩΝ ΚΑΤΩΦΛΙΩΝ
    thresh = [140,145,150]

    # Extract the observations WL forecasted values and stored in the array yValues
    Obs_yV_length = len(response_forecast['Datastreams'][0]['Observations'])

    Obs_yv = []
    for iter in range(0, Obs_yV_length):
        Obs_yv += [response_forecast['Datastreams'][0]['Observations'][iter]['result']]

    # Find all the maximum of the Obs_yv and its positions
    Obs_yv_max = max(Obs_yv)
    maxIndexList = [i for i,j in enumerate(Obs_yv) if j == Obs_yv_max]
    first_max_pos = [min(maxIndexList)]  # considers only the first maximum value

    for pos in range(0, len(first_max_pos)):        #range(0, len(maxIndexList)):

        # Calculate the Crisis Classification Level for each River Section
        #   If the maximum value exceeds one of the predefined thresholds then
        #   it stored in the topic (flag_extreme=True), otherwise it ignores (flag_extreme = False)
        #
        flag_extreme = False

        # Compare maximum observation of the Obs_yv with the Thresholds
        if Obs_yv_max >= thresh[0] and Obs_yv_max < thresh[1]:
            meas_color += ['#FFFF00']  # yellow
            meas_note += ['Water level overflow: exceeding of the 1st alarm threshold']
            flag_extreme = True
        elif Obs_yv_max >= thresh[1] and Obs_yv_max < thresh[2]:
            meas_color += ['#FFA500']  # orange
            meas_note += ['Water level overflow: exceeding of the 2nd alarm threshold']
            flag_extreme = True
        elif Obs_yv_max >= thresh[2]:
            meas_color += ['#FF0000']  # red
            meas_note += ['Water level overflow: exceeding of the 3rd alarm threshold']
            flag_extreme = True
        else:
            #meas_color += ['#00FF00']  # green
            #meas_note += ['Water level OK']
            flag_extreme = False
            #print("'Water level OK': IGNOR max ", Obs_yv_max, " of river section:", counter )

        if flag_extreme == True:
            max_yValues += [Obs_yv_max]
            dataSeriesID += [riverSections["value"][counter]['@iot.id']]  # counter + 1
            dataSeriesName += [riverSections["value"][counter]['name']]

            # Find details regarding the maximum observation and stored them in the corresponding arrays
            item = response_forecast['Datastreams'][0]['Observations'][pos]
            max_measurementID += [item['@iot.id']]
            max_measurementTimeStamp += [item['phenomenonTime'].replace('.000Z', "Z")]
            #print("'Water level EXCEED': max ", Obs_yv_max, " of river section:", counter )


# End Timing Step 2
end_step2 = time.time()
time_duration_step2 = end_step2 - start_step2


#--------------------------------------------------------------------------------------------
#  STEP 3: Creates the TOPIC_104_METRIC_REPORT
#--------------------------------------------------------------------------------------------
#
# Create the TOPIC 104 (json format) for the maximum value of predicted water levels
# in the time interval defined by the 'dates' or for the lastRun of AMICO's program
# for all river sections.
#

# Start Timing Step 3
start_step3 = time.time()

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

# Arbitrary position in the center of Vicenca (longtitude, latitude)
position = ["11.53542","45.54547"]

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

# Call the class Top104_Metric_Report to create an object data of this class
#
data = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                            dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                            lang, dataStreamCategory, dataStreamSubCategory, position)

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
flname = directory + "/" + 'TOP104_forecasts_riverSections.txt'
with open(flname, 'w') as outfile:
    json.dump(top104_forecast, outfile, indent=4)

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


#----------------------------------------------------------------------------------------
# Create new Producer instance using provided configuration message (dict data).

producer = BusProducer()

# Decorate terminal
print('\033[95m' + "\n***********************")
print("*** CRCL SERVICE v1.0 ***")
print("***********************\n" + '\033[0m')

print('First message: Max Predicted Water Level value has been forwarded to logger!')
producer.send("TOP104_METRIC_REPORT", top104_forecast)

topics = ['TOP104_METRIC_REPORT']
crcl_service = CRCLService(listen_to_topics=topics)
crcl_service.run_service()
