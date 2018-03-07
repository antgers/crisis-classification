# Implements the 1st algorithm of Crisis Classification module
# based on the predicted water levels from AMICO for all
# river sections in the next 54h starting a specific date/time

#----------------------------------------------------------------------------------------------------------
# Inputs: a) Time series of predicted water levels from AMICO for each one of the
#            river section in the next 54h starting a specific date/time
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
import json
import os, errno
from pathlib import Path
from datetime import datetime, timedelta

from Top104_Metric_Report import Top104_Metric_Report
from Create_Queries import extract_forecasts
from Create_Queries import extract_stations_river

# Create a directory to store the output files and TOPICS
root_path = Path.cwd()
directory = "TOPICS"
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

# https://beaware.server.de/SensorThingsService/v1.0/Things
# ? $filter=properties/type%20eq%20%27riverSection%27
# & $select=id,name,properties
# & $count=true
# & $top=1000

SensorThings = [SensorThingEntities[0]]

filt_vals = 'riverSection'
sel_vals = ['id', 'name', 'description', 'properties']

riverSections = extract_stations_river(service_root_URI, SensorThings, filt_vals, sel_vals)

# write json (data) to output file
flname = directory + "/" + 'response_riverSections.txt'
with open(flname, 'w') as outfile:
    json.dump(riverSections, outfile)

count = riverSections["@iot.count"]
print(" Total River Sections = ", count)

#------------------------------------------------------------------------------------------------
# STEP 2: Extract predicted water levels from AMICO for a specific river section in the next 54h
#------------------------------------------------------------------------------------------------
# Extract one measurement (forecast for water river level) from one station at specific date/time
#
# ex. Things id 390 -> River section Astico m .00
#     Date -> 2018-01-26T08:00:00.000Z

for counter in range(0, 1): #count):

    print('\n***************************')
    print('River Section name = ', riverSections["value"][counter]['name'])
    print('River Section id = ', riverSections["value"][counter]['@iot.id'])
    print(' Counter = ', counter)

    ids = {'th_id': str(riverSections["value"][counter]['@iot.id']) }

    SensorThings = [SensorThingEntities[0], SensorThingEntities[1], SensorThingEntities[3], SensorThingEntities[5]]
    sel_vals = {'dstr_sel': ['id', 'name', 'properties'], 'obs_sel': ['result', 'phenomenonTime', 'id']}
    filt_args={'obs_filt': ['phenomenonTime']}
    dates = ['2018-01-26T08:00:00.000Z', '2018-01-28T14:00:00.000Z']
    filt_vals={'obs_filt_vals': dates}
    ord_vals = ['phenomenonTime']

    flag_last_run = True #False

    if flag_last_run == False:
        response_forecast = extract_forecasts(service_root_URI, SensorThings, ids, sel_vals, ord_vals, filter_args=filt_args, filter_vals=filt_vals)
    else:
        response_forecast = extract_forecasts(service_root_URI, SensorThings, ids, sel_vals, ord_vals, last_run=flag_last_run)

    # write json (data) to output file
    flname = directory + "/" + 'response_forecast_' + riverSections["value"][counter]['name'].replace(" ", "") + ".txt"
    with open(flname, 'w') as outfile:
        json.dump(response_forecast, outfile)


    # Extract the thresholds of the response of riverSections query correspond to the specific river section
    thresh = [riverSections["value"][counter]['properties']['treshold1'],
              riverSections["value"][counter]['properties']['treshold2'],
              riverSections["value"][counter]['properties']['treshold3']]

    #--------------------------------------------------------------------------------------------
    #  STEP 3: Calculate the Crisis Classification level and creates the TOPIC_104_METRIC_REPORT
    #--------------------------------------------------------------------------------------------
    #
    # (A) Create the Topic 104 from the whole forecast values of water levels in the particular
    #   river section in the specific date
    #
    msgIdent = "5433dfde68"
    i = datetime.now()
    sent_dateTime = i.strftime('%Y/%m/%d %H:%M:%S')
    status = "Actual"
    actionType = "Update"
    scope = "Public"
    district = response_forecast['name']
    code = 20190617001

    dataStreamGener = "CRCL"
    dataStreamID = response_forecast["Datastreams"][0]["@iot.id"]
    dataStreamName = response_forecast["Datastreams"][0]["name"]
    dataStreamDescript = response_forecast["Datastreams"][0]["properties"]["type"] + " and " + str(response_forecast["Datastreams"][0]["properties"]["lastRunId"])
    lang = "it-IT"
    dataStreamCategory = "Met"
    dataStreamSubCategory = "Flood"

    coord_0 = response_forecast['Locations'][0]['location']['coordinates'][0]
    coord_1 = response_forecast['Locations'][0]['location']['coordinates'][1]
    position = [coord_0, coord_1]

    dataSeriesID = '1'
    dataSeriesName = response_forecast['name']

    # Call the class Top104_Metric_Report to create an object data of this class
    #
    data = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                 dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                                 lang, dataStreamCategory, dataStreamSubCategory, position,
                                 dataSeriesID, dataSeriesName)

    data.topic_note = "Threshold_1=" + str(thresh[0]) + ", " + "Threshold_2=" + str(thresh[1]) + ", " + "Threshold_3=" + str(thresh[2])

    # create the header of the object
    data.create_dictHeader()

    # create the measurements of the object
    #
    for item in response_forecast['Datastreams'][0]['Observations']:
         data.topic_yValue += [item['result']]
         data.topic_measurementID += [item['@iot.id']]
         data.topic_measurementTimeStamp += [item['phenomenonTime']]

         if 'xValue' in item:
             data.topic_xValue += [item['xValue']]
         else:
             data.topic_xValue += [item['phenomenonTime']]

         if item['result'] >= thresh[0] and item['result'] < thresh[1]:
             data.topic_meas_color += ['#FFFF00']  # yellow
             data.topic_meas_note += ['Water level overflow: exceeding of the 1st alarm threshold']
         elif item['result'] >= thresh[1] and item['result'] < thresh[2]:
             data.topic_meas_color += ['#FFA500']  # orange
             data.topic_meas_note += ['Water level overflow: exceeding of the 2nd alarm threshold']
         elif item['result'] >= thresh[2]:
             data.topic_meas_color += ['#FF0000']  # red
             data.topic_meas_note += ['Water level overflow: exceeding of the 3rd alarm threshold']
         else:
             data.topic_meas_color += ['#00FF00']  # green
             data.topic_meas_note += ['Water level OK']

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

    #-------------------------------------------------------------------------------------------
    # (B) Call function to create the TOPIC 104 (json format) for response_forecast
    #     for the maximum value of predicted water levels in the time interval defined by the 'dates'
    #     for a particular river section.

    yValues_lenght = len(data.topic_yValue)
    pos, yValues_max = max(enumerate(data.topic_yValue), key=lambda x: x[1])

    # Call the class Top104_Metric_Report to create an object data of this class
    #
    max_predWL_item = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                 dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                                 lang, dataStreamCategory, dataStreamSubCategory, position,
                                 dataSeriesID, dataSeriesName)

    max_predWL_item.topic_note = "Threshold_1=" + str(thresh[0]) + ", " + "Threshold_2=" + str(thresh[1]) + ", " + "Threshold_3=" + str(thresh[2])

    # create the header of the object
    max_predWL_item.create_dictHeader()

    max_predWL_item.topic_yValue = [yValues_max]
    item = response_forecast['Datastreams'][0]['Observations'][pos]
    max_predWL_item.topic_measurementID = [item['@iot.id']]
    max_predWL_item.topic_measurementTimeStamp = [item['phenomenonTime']]

    if 'xValue' in item:
        max_predWL_item.topic_xValue = [item['xValue']]
    else:
        max_predWL_item.topic_xValue = [item['phenomenonTime']]

    if yValues_max >= thresh[0] and yValues_max < thresh[1]:
        max_predWL_item.topic_meas_color += ['#FFFF00']  # yellow
        max_predWL_item.topic_meas_note += ['Water level overflow: exceeding of the 1st alarm threshold']
    elif yValues_max >= thresh[1] and yValues_max < thresh[2]:
        max_predWL_item.topic_meas_color += ['#FFA500']  # orange
        max_predWL_item.topic_meas_note += ['Water level overflow: exceeding of the 2nd alarm threshold']
    elif yValues_max >= thresh[2]:
        max_predWL_item.topic_meas_color += ['#FF0000']  # red
        max_predWL_item.topic_meas_note += ['Water level overflow: exceeding of the 3rd alarm threshold']
    else:
        max_predWL_item.topic_meas_color += ['#00FF00']  # green
        max_predWL_item.topic_meas_note += ['Water level OK']

    # call class function
    max_predWL_item.create_dictMeasurements()

    # create the body of the object
    max_predWL_item.create_dictBody()

    # create the TOP104_METRIC_REPORT as json
    top104_max_predWL = {'header': max_predWL_item.header, 'body': max_predWL_item.body}

    # write json (top104_max_predWL) to output file
    flname = directory + "/" + 'TOP104_max_predWL_' + riverSections["value"][counter]['name'].replace(" ", "") + ".txt"
    with open(flname, 'w') as outfile:
        json.dump(top104_max_predWL, outfile, indent=4)

    #----------------------------------------------------------------------------------------
    # Create new Producer instance using provided configuration message (dict data).
    producer = BusProducer()

    # Decorate terminal
    print('\033[95m' + "\n***********************")
    print("*** CRCL SERVICE v1.0 ***")
    print("***********************\n" + '\033[0m')

    print('First message: Max Predicted Water Level value has been forwarded to logger!')
    producer.send("TOP104_METRIC_REPORT", top104_max_predWL)

    #print('Second message: All predicted WL values ')
    # producer.send("TOP104_METRIC_REPORT", top104_forecast)
    #
    # topics = ['TOP104_METRIC_REPORT']
    # crcl_service = CRCLService(listen_to_topics=topics)
    # crcl_service.run_service()

