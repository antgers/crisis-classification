from builtins import zip


class Top104_Metric_Report:
    'Class for manipulation (create, edit, delete) Topic 104 Metric Report'

    # Constractor of the class
    def __init__(self, msgIdentifier, sentUTC, status, actionType, scope, district, code,
                 dataStreamGenerator, dataStreamID, dataStreamName, dataStreamDescription, language,
                 dataStreamCategory, dataStreamSubCategory, position):
        # header variables
        self.topic_name = "TOP104_METRIC_REPORT"
        self.topic_MajorVersion = '0'
        self.topic_MinorVersion = '1'
        self.topic_sender = "CRCL"
        self.topic_msgIdentifier = msgIdentifier
        self.topic_sentUTC = sentUTC
        self.topic_status = status
        self.topic_actionType = actionType
        self.topic_scope = scope
        self.topic_district = district
        self.topic_code = code
        self.topic_references = ""
        self.topic_note = ""
        self.topic_specificSender = ""
        self.topic_recipients = ""
        # body variables
        self.topic_dataStreamGenerator = dataStreamGenerator
        self.topic_dataStreamID = dataStreamID
        self.topic_dataStreamName = dataStreamName
        self.topic_dataStreamDescription = dataStreamDescription
        self.topic_language = language
        self.topic_dataStreamCategory = dataStreamCategory
        self.topic_dataStreamSubCategory = dataStreamSubCategory
        self.topic_position = {"longitude": position[0], "latitude": position[1]}
        # measurement variables
        self.topic_measurementID = []
        self.topic_measurementTimeStamp = []
        self.topic_dataSeriesID = []
        self.topic_dataSeriesName = []
        self.topic_xValue = []
        self.topic_yValue = []
        self.topic_meas_color = []
        self.topic_meas_note = []
        self.header = None
        self.body = None
        self.measurements = []

    # Create the header of the class object
    def create_dictHeader(self):
        self.header = {"topicName": self.topic_name,
                       "topicMajorVersion": self.topic_MajorVersion,
                       "topicMinorVersion": self.topic_MinorVersion,
                       "sender": self.topic_sender,
                       "msgIdentifier": self.topic_msgIdentifier,
                       "sentUTC": self.topic_sentUTC,
                       "status": self.topic_status,
                       "actionType": self.topic_actionType,
                       "specificSender": self.topic_specificSender,
                       "scope": self.topic_scope,
                       "district": self.topic_district,
                       "recipients": self.topic_recipients,
                       "code": self.topic_code,
                       "note": self.topic_note,
                       "references": self.topic_references}

    # Create the body of the class object
    def create_dictBody(self):
        self.body = {"dataStreamGenerator": self.topic_dataStreamGenerator,
                     "dataStreamID": self.topic_dataStreamID,
                     "dataStreamName": self.topic_dataStreamName,
                     "dataStreamDescription": self.topic_dataStreamDescription,
                     "language": self.topic_language,
                     "dataStreamCategory": self.topic_dataStreamCategory,
                     "dataStreamSubCategory": self.topic_dataStreamSubCategory,
                     "position": {"longitude": self.topic_position["longitude"],
                                  "latitude": self.topic_position["latitude"]},
                     "measurements": self.measurements}

    # Create the list of measurements for the class object
    def create_dictMeasurements(self):
        for (cid, ctime, cx, cy, ccol, cnote, dsID,dsN) in zip(self.topic_measurementID,
                                                     self.topic_measurementTimeStamp,
                                                     self.topic_xValue,
                                                     self.topic_yValue,
                                                     self.topic_meas_color,
                                                     self.topic_meas_note,
                                                     self.topic_dataSeriesID,
                                                     self.topic_dataSeriesName):
            self.measurements += [{
                "measurementID": cid,
                "measurementTimeStamp": ctime,
                "dataSeriesID": dsID,
                "dataSeriesName": dsN,
                "xValue": cx,
                "yValue": cy,
                "color": ccol,
                "note": cnote
           }]


    # Print an object of the class  -- OBSOLETE MAYBE WRONG
    # def displayTopic104(self):
    #      print("\n Topic 104 is: \n")
    #      print('Name: ', self.topic_name)
    #      print('Major Version: ', self.topic_MajorVersion)
    #      print('Minor Version: ', self.topic_MinorVersion)
    #      print('Sender: ', self.topic_sender)
    #      print('Status: ', self.topic_status)
    #      print('MsgIdentifier: ', self.topic_msgIdentifier)
    #      print('Sent UTC: ', self.topic_sentUTC)
    #      print('Action Type: ', self.topic_actionType)
    #      print('Scope: ', self.topic_scope)
    #      print('District: ', self.topic_district)
    #      print('Code: ', self.topic_code)
    #      print('References: ', self.topic_references)
    #      print('Note:', self.topic_note)
    #      print('Specific sender: ', self.topic_specificSender)
    #      print('Recipients :', self.topic_recipients)
    #      print('DataStreamGenerator: ', self.topic_dataStreamGenerator)
    #      print('Stream ID:', self.topic_dataStreamID)
    #      print('DataStream Name: ', self.topic_dataStreamName)
    #      print('DataStream Description: ', self.topic_dataStreamDescription)
    #      print('Language: ', self.topic_language)
    #      print('DataStreamCategory: ', self.topic_dataStreamCategory)
    #      print('DataStreamSubCategory:', self.topic_dataStreamSubCategory)
    #      print('Position: [', self.topic_position["latitude"], ',', self.topic_position["longitude"], ']')
    #      print('Measurements: [', self.measurements, ']')