# Implements the 1st algorithm of Crisis Classification module
# based on the predicted water levels from AMICO for a specific
# river section in the next 54h

#----------------------------------------------------------------------------------------------------------
# Inputs: a) Time series of predicted water levels from AMICO for a specific river section in the next 54h
#         b) Thresholds for the particular river section
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
from datetime import datetime, timedelta


from Top104_Metric_Report import Top104_Metric_Report
from Create_Queries import extract_forecasts

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


#------------------------------------------------------------------------------------------------
# STEP 1: Extract predicted water levels from AMICO for a specific river section in the next 54h
#------------------------------------------------------------------------------------------------
# Extract one measurement (forecast for water river level) from one station at specific date/time
#
# Things id 390 -> River section Astico m .00
# Date -> 2018-01-26T08:00:00.000Z

ids = {'th_id':'390'}
SensorThings = [SensorThingEntities[0], SensorThingEntities[1], SensorThingEntities[3], SensorThingEntities[5]]
sel_vals = {'dstr_sel': ['id', 'name', 'properties'], 'obs_sel': ['result', 'phenomenonTime', 'id']}
filter_args={'obs_filt': ['phenomenonTime']}
dates = ['2018-01-26T08:00:00.000Z', '2018-01-28T14:00:00.000Z']
filter_vals={'obs_filt_vals': dates}


response_forecast = extract_forecasts(service_root_URI, SensorThings, ids, filter_args, filter_vals, sel_vals)

# write json (data) to output file
with open('response_forecast_endJan2018.txt', 'w') as outfile:
    json.dump(response_forecast, outfile)


# Extract the thresholds of the response
thresh = [response_forecast["properties"]["treshold1"],
          response_forecast["properties"]["treshold2"],
          response_forecast["properties"]["treshold3"]]


#--------------------------------------------------------------------------------------------
#  STEP 2: Calculate the Crisis Classification level and creates the TOPIC_104_METRIC_REPORT
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
with open('TOP104_forecast_outjson_endJan2018.txt', 'w') as outfile:
    json.dump(top104_forecast, outfile, indent=4)


#-------------------------------------------------------------------------------------------
# (B) Call function to create the TOPIC 104 (json format) for response_forecast
#     for the maximum value of predicted water levels in the time interval defined by the 'dates'
#     for a particular river section.
#

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
with open('TOP104_max_predWL_outjson.txt', 'w') as outfile:
    json.dump(top104_max_predWL, outfile, indent=4)


#----------------------------------------------------------------------------------------
# Create new Producer instance using provided configuration message (dict data).
producer = BusProducer()

# Decorate terminal
print('\033[95m' + "\n***********************")
print("*** CRCL SERVICE v1.0 ***")
print("***********************\n" + '\033[0m')

print('\n First message: Max Predicted Water Level value ')
producer.send("TOP104_METRIC_REPORT", top104_max_predWL)

print('Second message: All predicted WL values ')
producer.send("TOP104_METRIC_REPORT", top104_forecast)

topics = ['TOP104_METRIC_REPORT']
crcl_service = CRCLService(listen_to_topics=topics)
crcl_service.run_service()

