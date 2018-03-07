def create_json():

    import json
    #import io

    #try:
    #    to_unicode = unicode
    #except NameError:
    #    to_unicode = str


    # Header
    topic_name = "TOP104_METRIC_REPORT"
    topic_sender = "CRCL"
    topic_status = "Actual"
    topic_sentUTC = "2018-01-18T12:00:00Z"
    topic_actionType = "Update"
    topic_scope = "Public"
    topic_district = "Vicenza"
    topic_code = 20190617001
    topic_references = "http://object-store-app.eu-gb.mybluemix.net/objectStorage?file=CRCL542853.json"
    topic_msgIdentifier = "5443fafde2853ffdd"

    # Body
    topic_dataStreamGenerator = "CRCL"
    topic_dataStreamID = "CrisisLevelVicenza2018v1"
    topic_dataStreamName = "Flood Crisis Level"
    topic_dataStreamDescription = "Integrated crisis level assessment for flood scenario"
    topic_lang = "it-IT"
    topic_dataStreamCategory = "Met"
    topic_dataStreamSubCategory = "Flood"
    pos_lat_long = [45.552475, 11.549126]


    top104_dict = {
     "header": {
            "topicName": topic_name,
            "topicMajorVersion": 0,
            "topicMinorVersion": 1,
            "sender": topic_sender,
            "msgIdentifier": topic_msgIdentifier,
            "sentUTC": topic_sentUTC,
            "status": topic_status,
            "actionType": topic_actionType,
            "specificSender": "",
            "scope": topic_scope,
            "district": topic_district,
            "recipients": "",
            "code": topic_code,
            "note": "",
            "references": topic_references
     },
     "body": {
            "dataStreamGenerator": topic_dataStreamGenerator,
            "dataStreamID": topic_dataStreamID,
            "dataStreamName": topic_dataStreamName,
            "dataStreamDescription": topic_dataStreamDescription,
            "language": topic_lang,
            "dataStreamCategory": topic_dataStreamCategory,
            "dataStreamSubCategory": topic_dataStreamSubCategory,
            "position": {
                "latitude": pos_lat_long[0],
                "longitude": pos_lat_long[1]
            }
     }
    }

    print(json.dumps(top104_dict))

    #with io.open('outjsonTOP104.json', 'w', encoding='utf8') as outfile:
    #    str_ = json.dumps(top104_dict,
    #                 indent=4, sort_keys=False,
    #                  separators=(',', ': '), ensure_ascii=False)
    #    outfile.write(to_unicode(str_))

    return top104_dict