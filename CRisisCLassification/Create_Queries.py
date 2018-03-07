import urllib.request
import json


# ----- QUERY extract 'Air Temperature' from a particular Thing (station)
# https://beaware.server.de/SensorThingsService/v1.0/
#   Things(37)?
#   $select=id, name
#  & $expand=Datastreams(
#      $filter=id eq 84
#     ;$expand=Observations(
#       $select=result, phenomenonTime, resultTime, id
#      ;$filter=phenomenonTime ge 2017-08-01T00:00:00.000Z and phenomenonTime le 2017-11-01T00:00:00.000Z
#      ;$top=10000
#     )
#   )
def extract_air_temp(service_root_URI, SensorThings, ids, dates, filter_vals, sel_vals):

    op_filter = '$filter'
    op_expand = '$expand'
    op_select = '$select'
    op_top = '$top'
    op_resform = '$resultFormat'
    op_eq = '%20eq%20'
    op_le = '%20le%20'
    op_ge = '%20ge%20'
    op_and = '%20and%20'

    resource_path = service_root_URI + SensorThings[0] + '(' + ids['th_id'] + ')'

    sel1 = op_select + "=" + sel_vals['th_sel'][0] + "," + sel_vals['th_sel'][1]

    sel2 = op_select + "=" + sel_vals['obs_sel'][0] + "," + sel_vals['obs_sel'][1] + "," + \
           sel_vals['obs_sel'][2] + "," + sel_vals['obs_sel'][3]

    filt1 = op_filter + "=" + filter_vals['dstr_filt'][0] + op_eq + ids['dstr']

    filt2 = op_filter + "=" + filter_vals['obs_filt'][0] + op_ge + dates[0] + op_and + \
            filter_vals['obs_filt'][0] + op_le + dates[1]


    dataArrayFormat = op_resform + "=" + 'dataArray'

    lev_q2 = filt1 + ';' + op_expand + "=" + SensorThings[2] + "(" + sel2 + ";" + filt2 + ";" + \
             op_top + "=" + '10000' + ";" + dataArrayFormat + ")"

    query = resource_path + '?' + sel1 + "&" + op_expand + "=" + SensorThings[1] + "(" + lev_q2 + ")"
    print(query)

    # read from url - execute the query and the response is stored to json obj
    with urllib.request.urlopen(query) as url:
        response = json.loads(url.read().decode())

    return(response)


#----------------------------------------------------------------------------------------------
# ----- QUERY extract the Things which are weather stations or/and rivers or/and riverSections
# https://beaware.server.de/SensorThingsService/v1.0/
# Things
# ? $filter=properties/type%20eq%20%27station%27  [ river and/or riverSection ]
# & $select=description,id,name,properties
#
def extract_stations_river(service_root_URI, SensorThings, filter_vals=None, sel_vals=None):

    # if filter_vals does not exists then the function returns all the Things (river and station)
    # if sel_vals does not exists then the function returns all the properties of each Thing

    op_filter = '$filter'
    op_select = '$select'
    op_top = '$top'
    op_count = '$count'
    op_resform = '$resultFormat'
    op_eq = '%20eq%20'

    resource_path = service_root_URI + SensorThings[0]

    if filter_vals != None:
       filt_cond = 'properties/type' + op_eq + "%27" + filter_vals + "%27"

    if sel_vals != None:
       sel1 = op_select + "="
       len_sel_vals = len(sel_vals)

       for i in range(len(sel_vals)-1):
           sel1 = sel1 + sel_vals[i] + ","
       sel1 = sel1 + sel_vals[len(sel_vals)-1]

    if (filter_vals != None and sel_vals != None):
        query = resource_path + "?" + op_filter + "=" + filt_cond + "&" + sel1
    elif (filter_vals != None and sel_vals == None):
        query = resource_path + "?" + op_filter + "=" + filt_cond
    elif (filter_vals == None and sel_vals != None):
        query = resource_path + "?" + sel1
    else:
        query = resource_path

    query = query + "&" + op_count + "=" + "true" + "&" + op_top + "=" + '10000'

    print("\n", query, "\n")

    # read from url - execute the query and the response is stored to json obj
    with urllib.request.urlopen(query) as url:
        response = json.loads(url.read().decode())

    return(response)

#------------------------------------------------------------------------------------------------------------
# ----- QUERY extract one measurement (forecast for river water level) from one station
#       at specific date/time or an interval of dates/time
#   OR
#       extracts the last run of AMICO if boolean last_run is TRUE
#
# https://beaware.server.de/SensorThingsService/v1.0/
# Things(390)
# ? $expand=Locations, Datastreams($select=id,name,properties
# ; $expand=Observations($select=result,phenomenonTime, id
# ; $filter= phenomenonTime%20ge%202018-01-26T14:00:00.000Z and
#            phenomenonTime%20le%202018-01-27T14:00:00.000Z
# ; $orderby=phenomenonTime%20asc
# ; $top=1000) ) )
# or
# ; $filter= parameters/runId%20eq%20Datastream/properties/lastRunId%20add%200
# ; $orderby= phenomenonTime%20asc
# ; $top=1000) ) )
#
def extract_forecasts(service_root_URI, SensorThings, ids, sel_vals, ord_val, last_run=None, filter_args=None, filter_vals=None):

    op_filter = '$filter'
    op_select = '$select'
    op_expand = '$expand'
    op_order = '$orderby'
    op_top = '$top'
    op_eq = '%20eq%20'
    op_add = '%20add%20'
    op_le = '%20le%20'
    op_ge = '%20ge%20'
    op_and = '%20and%20'

    resource_path = service_root_URI + SensorThings[0] + '(' + ids['th_id'] + ')'

    if sel_vals['dstr_sel'] != None:
       sel1 = op_select + "="
       len_dstr_sel = len(sel_vals['dstr_sel'])

       for i in range(len_dstr_sel-1):
           sel1 = sel1 + sel_vals['dstr_sel'][i] + ","
       sel1 = sel1 + sel_vals['dstr_sel'][len_dstr_sel-1]

    if sel_vals['obs_sel'] != None:
       sel2 = op_select + "="
       len_obs_sel = len(sel_vals['obs_sel'])

       for i in range(len_obs_sel-1):
           sel2 = sel2 + sel_vals['obs_sel'][i] + ","
       sel2 = sel2 + sel_vals['obs_sel'][len_obs_sel-1]

    filt1 = op_filter + "="
    if last_run != None:
        # extract the last AMICO run
        filt1 = filt1 + "parameters/runId" + op_eq + "Datastream/properties/lastRunId" + op_add + '0'
    else:
        if filter_args['obs_filt'] != None:
            len_obs_filt = len(filter_args['obs_filt'])
            if len_obs_filt == 1:
               len_obs_filt_vals = len(filter_vals['obs_filt_vals'])
               if len_obs_filt_vals == 1:
                   filt1 = filt1 + filter_args['obs_filt'][0] + op_eq + filter_vals['obs_filt_vals'][0]
               else:
                   filt1 = filt1 + filter_args['obs_filt'][0] + op_ge + filter_vals['obs_filt_vals'][0] + \
                           op_and + filter_args['obs_filt'][0] + op_le + filter_vals['obs_filt_vals'][1]
            else:
               for i in range(1, len_obs_filt):
                   len_obs_filt_vals = len(filter_vals['obs_filt_vals'])
                   if len_obs_filt_vals == 1:
                       filt1 = filt1 + filter_args['obs_filt'][i-1] + op_eq + filter_vals['obs_filt_vals'][i-1]
                   else:
                       filt1 = filt1 + filter_args['obs_filt'][i-1] + op_ge + filter_vals['obs_filt_vals'][0] + \
                               op_and + filter_args['obs_filt'][i-1] + op_le + filter_vals['obs_filt_vals'][1]

    lev_q2 = ';' + op_expand + "=" + SensorThings[3] + \
            "(" + sel2 + ";" + filt1 + ";" + op_order + "=" + ord_val[0] + ";" + op_top + "=" + '1000' + ")"


    query = resource_path + '?' + op_expand + "=" + SensorThings[1] + "," + SensorThings[2] + "(" + sel1 + lev_q2 + ")"
    #print(query)

    # read from url - execute the query and the response is stored to json obj
    with urllib.request.urlopen(query) as url:
        response = json.loads(url.read().decode())

    return(response)

#-------------------------------------------------------------------------------------------------
#------ QUERY extracts data from one Weather Station specified by id_WSt
#       and one of the two Datastreams by starting with the word 'Precipitation' or 'Water Level'
#       at particular date/time or last date measurement depending on the value of last_meas. If it is
#       True then the last measurement is extracted from the Datastream PhenomenonTime . Otherwise,
#       the user defined dates are utilised.
#
# https://beaware.server.de/SensorThingsService/v1.0/Things?
# $filter=id eq id_WSt
# &$select=id,name,description
# &$expand=Datastreams($filter=startswith(name,%27Precipitation%27)
# ;$select=id,phenomenonTime,name
# ;$expand=Observations(
# $select=result,phenomenonTime
# ;$filter=phenomenonTime ge 2018-02-01T00:00:00.000Z and phenomenonTime le 2018-02-13T00:00:00.000Z
# ;$orderby=phenomenonTime desc))
#
# other options:
#
# %20or%20startswith(name,%27Water%20Level%27))
# ;$filter=phenomenonTime eq 2018-02-11T23:00:00.000Z
#
def extract_from_WS_Sensors(service_root_URI, SensorThings, sel_vals, ord_val, filter_args, filter_vals):

    op_filter = '$filter'
    op_select = '$select'
    op_expand = '$expand'
    op_order = '$orderby'
    op_top = '$top'
    op_eq = '%20eq%20'
    op_add = '%20add%20'
    op_le = '%20le%20'
    op_ge = '%20ge%20'
    op_and = '%20and%20'

    resource_path = service_root_URI + SensorThings[0]

    filt_Thing = op_filter + "=" + filter_args['thing_filt'][0] + op_eq + filter_vals['thing_filt'][0]

    if sel_vals != None:
        len_sel_vals = len(sel_vals)
        sel_vals_keys = list(sel_vals.keys())

        if sel_vals['thing_sel'] != None:
            sel_Thing = op_select + "="
            len_thing_sel = len(sel_vals['thing_sel'])

        for i in range(len_thing_sel-1):
            sel_Thing = sel_Thing + sel_vals['thing_sel'][i] + ","
        sel_Thing = sel_Thing + sel_vals['thing_sel'][len_thing_sel-1]

        if sel_vals['dstr_sel'] != None:
            sel_Datastream = op_select + "="
            len_dstr_sel = len(sel_vals['dstr_sel'])

        for i in range(len_dstr_sel-1):
            sel_Datastream = sel_Datastream + sel_vals['dstr_sel'][i] + ","
        sel_Datastream = sel_Datastream + sel_vals['dstr_sel'][len_dstr_sel-1]

        if sel_vals['obs_sel'] != None:
            sel_Obs = op_select + "="
            len_obs_sel = len(sel_vals['obs_sel'])

        for i in range(len_obs_sel-1):
            sel_Obs = sel_Obs + sel_vals['obs_sel'][i] + ","
        sel_Obs = sel_Obs + sel_vals['obs_sel'][len_obs_sel-1]

    if len(filter_args['dstr_filt'])==1:
        filt_Datastream = op_filter + "=" + 'startswith(' + filter_args['dstr_filt'][0] + "," + "%27"+ filter_vals['dstr_filt'][0] + "%27" + ")"

    filt_Obs = op_filter + "="
    if filter_args['obs_filt'] != None:
        len_obs_filt = len(filter_args['obs_filt'])

        if len_obs_filt == 1:
            len_obs_filt_vals = len(filter_vals['obs_filt_vals'])
            if len_obs_filt_vals == 1:
                filt_Obs = filt_Obs + filter_args['obs_filt'][0] + op_eq + filter_vals['obs_filt_vals'][0]
            else:
                filt_Obs = filt_Obs + filter_args['obs_filt'][0] + op_ge + filter_vals['obs_filt_vals'][0] + \
                           op_and + filter_args['obs_filt'][0] + op_le + filter_vals['obs_filt_vals'][1]


    query = resource_path + '?' + filt_Thing + '&' + sel_Thing + '&' + \
            op_expand + "=" + SensorThings[1] + "(" + filt_Datastream + ";" + sel_Datastream + ";" + \
            op_expand + "=" + SensorThings[2] + "(" + sel_Obs + ";" + filt_Obs + ";" + \
            op_order + "=" + ord_val[0] + "%20desc" + ";" + op_top + "=" + '1000' + "))"

    print(query)

    # read from url - execute the query and the response is stored to json obj
    with urllib.request.urlopen(query) as url:
        response = json.loads(url.read().decode())

    return(response)

#-------------------------------------------------------------------------------------------------
#------ QUERY extracts datastream details from one Weather Station specified by StationID
#
# https://beaware.server.de/SensorThingsService/v1.0/Things?
# $ filter=id eq StationID
# & $expand=Datastreams($filter=startswith(name, %27Water%27)
# ; $select=id,phenomenonTime,name)
#
def extract_station_datastream(service_root_URI, SensorThings, sel_vals, filter_args, filter_vals):

    op_filter = '$filter'
    op_select = '$select'
    op_expand = '$expand'
    op_eq = '%20eq%20'

    resource_path = service_root_URI + SensorThings[0]

    filt_Thing = op_filter + "=" + filter_args['thing_filt'][0] + op_eq + filter_vals['thing_filt']
    filt_Dstr = op_filter + "=" + 'startswith(' + filter_args['dstr_filt'][0] + "," + "%27"+ filter_vals['dstr_filt'][0] + "%27" + ")"

    if sel_vals != None:
        if sel_vals['dstr_sel'] != None:
            sel_Datastream = op_select + "="
            len_dstr_sel = len(sel_vals['dstr_sel'])

        for i in range(len_dstr_sel - 1):
            sel_Datastream = sel_Datastream + sel_vals['dstr_sel'][i] + ","
        sel_Datastream = sel_Datastream + sel_vals['dstr_sel'][len_dstr_sel - 1]


    query = resource_path + '?' + filt_Thing + "&" + op_expand + "=" + SensorThings[1] + "(" + \
           filt_Dstr + ";" + sel_Datastream + ")"

    print(query)

    # read from url - execute the query and the response is stored to json obj
    with urllib.request.urlopen(query) as url:
        response = json.loads(url.read().decode())

    return(response)


#-------------------------------------------------------------------------------------------------
#------ QUERY extracts locations from a Weather Station specified by StationID
#
# https://beaware.server.de/SensorThingsService/v1.0/Things
# ? $filter=id eq StationID
# & $expand=Locations($select=location)
# & $select=name,id

def extract_station_location(service_root_URI, SensorThings, sel_vals, filter_args, filter_vals):

    op_filter = '$filter'
    op_select = '$select'
    op_expand = '$expand'
    op_eq = '%20eq%20'

    resource_path = service_root_URI + SensorThings[0]
    filt_Thing = op_filter + "=" + filter_args['thing_filt'][0] + op_eq + filter_vals['thing_filt']

    if sel_vals['thing_sel'] != None:
        sel_Thing = op_select + "="
        len_thing_sel = len(sel_vals['thing_sel'])

        for i in range(len_thing_sel-1):
            sel_Thing = sel_Thing + sel_vals['thing_sel'][i] + ","
        sel_Thing = sel_Thing + sel_vals['thing_sel'][len_thing_sel-1]

    if sel_vals['loc_sel'] != None:
        sel_Loc = op_select + "="
        len_loc_sel = len(sel_vals['loc_sel'])

        for i in range(len_loc_sel-1):
            sel_Loc = sel_Loc + sel_vals['loc_sel'][i] + ","
        sel_Loc = sel_Loc + sel_vals['loc_sel'][len_loc_sel-1]

    query = resource_path + '?' + filt_Thing + "&" + op_expand + "=" + \
            SensorThings[1] + "(" + sel_Loc + ")" + "&" +sel_Thing

    print(query)

    # read from url - execute the query and the response is stored to json obj
    with urllib.request.urlopen(query) as url:
        response = json.loads(url.read().decode())

    return(response)

#-------------------------------------------------------------------------------------------------
#------ QUERY extracts the ids, the names, the properties and the location of all river sections
#
# https://beaware.server.de/SensorThingsService/v1.0/Things
# ? $filter=properties/type%20eq%20%27riverSection%27
# & $select=id,name,properties
# & $expand=Locations($select=description,location)
# & $count=true
# & $top=1000

def extract_river_sections_loc(service_root_URI, SensorThings, filt_vals, sel_vals):
    op_filter = '$filter'
    op_select = '$select'
    op_expand = '$expand'
    op_eq = '%20eq%20'
    op_top = '$top'
    op_count = '$count'

    resource_path = service_root_URI + SensorThings[0]

    if filt_vals != None:
       filt_cond = 'properties/type' + op_eq + "%27" + filt_vals + "%27"

    filt_Thing = op_filter + "=" + filt_cond

    if sel_vals != None:
        len_sel_vals = len(sel_vals)
        sel_vals_keys = list(sel_vals.keys())

        if sel_vals['thing_sel'] != None:
            sel_Thing = op_select + "="
            len_thing_sel = len(sel_vals['thing_sel'])

        for i in range(len_thing_sel-1):
            sel_Thing = sel_Thing + sel_vals['thing_sel'][i] + ","
        sel_Thing = sel_Thing + sel_vals['thing_sel'][len_thing_sel-1]

        if sel_vals['loc_sel'] != None:
            sel_Loc = op_select + "="
            len_loc_sel = len(sel_vals['loc_sel'])

        for i in range(len_loc_sel-1):
            sel_Loc = sel_Loc + sel_vals['loc_sel'][i] + ","
        sel_Loc = sel_Loc + sel_vals['loc_sel'][len_loc_sel-1]

    query = resource_path + '?' + filt_Thing + "&" + sel_Thing + \
            "&" + op_expand + "=" + SensorThings[1] + "(" + sel_Loc + ")" + \
            "&" + op_count + "=" + "true" + "&" + op_top + "=" + '1000'

    print(query)

    # read from url - execute the query and the response is stored to json obj
    with urllib.request.urlopen(query) as url:
        response = json.loads(url.read().decode())

    return(response)