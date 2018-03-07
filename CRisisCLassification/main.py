# from bus import bus_consumer
# from bus import bus_producer

from bus.bus_producer import BusProducer
from bus.CRCL_service import CRCLService
import json

from datetime import datetime
from Top104_Metric_Report import Top104_Metric_Report
from Create_Queries import extract_air_temp
from Create_Queries import extract_stations_river
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

#--------------------------------------------------------------------------------------
# Create Query to extract fon one station a datastream of measurements
#
# Things Id = 37 corresponds to the station 'Grantorto'
# Datastream Id = 84 corresponds to the 'Air Temperature'
#
ids = {'th_id':'37','dstr':'84'}

#dates = ['2017-08-01T00:00:00.000Z', '2018-01-01T00:00:00.000Z']
dates = ['2018-01-20T00:00:00.000Z', '2018-01-30T00:00:00.000Z']
sel_vals = {'th_sel':['id', 'name'], 'obs_sel':['result', 'phenomenonTime', 'resultTime','id']}
filter_vals={'dstr_filt':['id'], 'obs_filt':['phenomenonTime']}

response_air_temp = extract_air_temp(service_root_URI, SensorThings, ids, dates, filter_vals, sel_vals)

# write json (data) to output file
with open('response_air_temp_outjson.txt', 'w') as outfile:
    json.dump(response_air_temp, outfile)

#------------------------------------------------------------------------------------------------
# Extract one measurement (forecast for water river level) from one station at specific date/time
#
# Things id 390 -> River section Astico m .00
# Date -> 2018-01-27T14:00:00.000Z


ids = {'th_id':'390'}
SensorThings = [SensorThingEntities[0], SensorThingEntities[1], SensorThingEntities[3], SensorThingEntities[5]]
sel_vals = {'dstr_sel': ['id', 'name', 'properties'], 'obs_sel': ['result', 'phenomenonTime', 'id']}
filter_args={'obs_filt': ['phenomenonTime']}
dates = ['2018-01-27T14:00:00.000Z']
filter_vals={'obs_filt_vals': dates}

response_forecast = extract_forecasts(service_root_URI, SensorThings, ids, filter_args, filter_vals, sel_vals)

# write json (data) to output file
with open('response_forecast.txt', 'w') as outfile:
    json.dump(response_forecast, outfile)

#-------------------------------------------------------------------------------------------
# Extract one measurement (forecast for water river level) from one station at specific date
# ex. date = 2018-01-27 to 2018-01-28

dates = ['2018-01-27T00:00:00.000Z', '2018-01-28T00:00:00.000Z']
filter_vals={'obs_filt_vals': dates}
response_forecast_oneday = extract_forecasts(service_root_URI, SensorThings, ids, filter_args, filter_vals, sel_vals)

# write json (data) to output file
with open('response_forecast_oneday.txt', 'w') as outfile:
    json.dump(response_forecast_oneday, outfile)


#------------------------------------------------------------------------------------
#
#   C R E A T E     T O P I C S   1 0 4
#--------------------------------------------------------------------------------------
# Create the Topic 104 from the inputs

# Call function to create the TOPIC 104 (json format) for response_air_temp

msgIdent = "5433dfde68"
i = datetime.now()
sent_dateTime = i.strftime('%Y/%m/%d %H:%M:%S')
status = "Actual"
actionType = "Update"
scope = "Public"
district = "Vicenza"
code = 20190617001

dataStreamGener = "CRCL"
dataStreamID = response_air_temp["Datastreams"][0]["@iot.id"]
dataStreamName = response_air_temp["Datastreams"][0]["name"]
dataStreamDescript = response_air_temp["Datastreams"][0]["description"]
lang = "it-IT"
dataStreamCategory = "Met"
dataStreamSubCategory = "Flood"
position = [45.552475, 11.549126]

dataSeriesID = '1'
dataSeriesName = 'Grantorto'

# Call the class Top104_Metric_Report to create an object data of this class
#
data = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                            dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                            lang, dataStreamCategory, dataStreamSubCategory, position,
                            dataSeriesID, dataSeriesName)

# create the header of the object
data.create_dictHeader()

# create the measurements of the object
#
for item in response_air_temp['Datastreams'][0]['Observations']:
    data.topic_yValue += [item['result']]
    data.topic_measurementID += [item['@iot.id']]
    data.topic_measurementTimeStamp += [item['phenomenonTime']]

    data.topic_meas_color += [""] #[None]
    data.topic_meas_note += [""] #[None]

    if 'xValue' in item:
        data.topic_xValue += [item['xValue']]
    else:
        data.topic_xValue += [item['phenomenonTime']]

# call class function
data.create_dictMeasurements()

# create the body of the object
data.create_dictBody()

# create the TOP104_METRIC_REPORT as json
top104 = {'header': data.header, 'body': data.body}

# write json (top104) to output file
with open('TOP104_outjson.txt', 'w') as outfile:
    json.dump(top104, outfile, indent=4)


#--------------------------------------------------------------------------
# Call function to create the TOPIC 104 (json format) for response_forecast
# for one measurement - date/time

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

# create the header of the object
data.create_dictHeader()

# create the measurements of the object
#

thresh = [response_forecast["properties"]["treshold1"],
          response_forecast["properties"]["treshold2"],
          response_forecast["properties"]["treshold3"]]

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
with open('TOP104_forecast_outjson.txt', 'w') as outfile:
    json.dump(top104_forecast, outfile, indent=4)

#--------------------------------------------------------------------------
# Call function to create the TOPIC 104 (json format) for response_forecast
# for more than one measurements for a period of time (ex. one day)

msgIdent = "5433dfde68"
i = datetime.now()
sent_dateTime = i.strftime('%Y/%m/%d %H:%M:%S')
status = "Actual"
actionType = "Update"
scope = "Public"
district = response_forecast['name']
code = 20190617001

dataStreamGener = "CRCL"
dataStreamID = response_forecast_oneday["Datastreams"][0]["@iot.id"]
dataStreamName = response_forecast_oneday["Datastreams"][0]["name"]
dataStreamDescript = response_forecast_oneday["Datastreams"][0]["properties"]["type"] + " and " + str(response_forecast_oneday["Datastreams"][0]["properties"]["lastRunId"])
lang = "it-IT"
dataStreamCategory = "Met"
dataStreamSubCategory = "Flood"

coord_0 = response_forecast_oneday['Locations'][0]['location']['coordinates'][0]
coord_1 = response_forecast_oneday['Locations'][0]['location']['coordinates'][1]
position = [coord_0, coord_1]

dataSeriesID = '1'
dataSeriesName = response_forecast_oneday['name']

# Call the class Top104_Metric_Report to create an object data of this class
#
data = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                            dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                            lang, dataStreamCategory, dataStreamSubCategory, position,
                            dataSeriesID, dataSeriesName)

# create the header of the object
data.create_dictHeader()

# create the measurements of the object
#

thresh = [response_forecast_oneday["properties"]["treshold1"],
          response_forecast_oneday["properties"]["treshold2"],
          response_forecast_oneday["properties"]["treshold3"]]

for item in response_forecast_oneday['Datastreams'][0]['Observations']:
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
top104_forecast_oneday = {'header': data.header, 'body': data.body}

# write json (top104_forecast_oneday) to output file
with open('TOP104_forecast_oneday_outjson.txt', 'w') as outfile:
    json.dump(top104_forecast_oneday, outfile, indent=4)

#----------------------------------------------------------------------------------------
# Create new Producer instance using provided configuration message (dict data).
producer = BusProducer()

# Decorate terminal
print('\033[95m' + "\n***********************")
print("*** CRCL SERVICE v1.0 ***")
print("***********************\n" + '\033[0m')

print('\n First message: \n')
producer.send("TOP104_METRIC_REPORT", top104)


#print('\n Second message: \n')
#producer.send("TOP104_METRIC_REPORT", top104_forecast)


#print('\n Third message: \n')
#producer.send("TOP104_METRIC_REPORT", top104_forecast_oneday)

topics = ['TOP104_METRIC_REPORT']
crcl_service = CRCLService(listen_to_topics=topics)
crcl_service.run_service()











# crcl_service.stop_service()