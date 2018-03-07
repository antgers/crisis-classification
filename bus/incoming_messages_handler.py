from bus.bus_producer import BusProducer
import sqlite3
import json


class IncomingMessagesHandler:
    def __init__(self):
        self.database = 'messages.sqlite'

        # Create producer
        self.producer = BusProducer()

    def process_database_messages(self):
        # Query for a message
        message = self.retrieve_a_message()

        # While there are messages
        while message != (None, None):

            # Process this message
            self.process_message(message[0], message[1])

            # Get next message
            message = self.retrieve_a_message()

    def process_message(self, message_id, message_text):
        message_json = None

        try:
            message_json = json.loads(message_text)
        except Exception as e:
            print(e)
        finally:
            # Delete message after being processed
            self.delete_message(message_id)

        # If message successfully parsed into json and contains a "body" field
        if (message_json is not None) and ('body' in message_json):

            # TODO: Add your functionality with the message here
            #  E.g.
            print('\n INSIDE INCOMING MESSAGES HANDLER \n')
            print(message_json['header']['topicName'])
            print(message_json['header']['status'])
            print(message_json['header']['sentUTC'])
            print(message_json['body']['dataStreamName'])

    def retrieve_a_message(self):
        try:
            con = sqlite3.connect(self.database)

            cur = con.cursor()
            cur.execute('SELECT MIN(id), message FROM requests')

            result = cur.fetchone()

            cur.close()

            return result

        except sqlite3.Error as e:
            print("Error %s:" % e.args[0])
            return False

    def delete_message(self, message_id):
        try:
            con = sqlite3.connect(self.database)

            with con:
                cur = con.cursor()
                cur.execute("DELETE FROM requests WHERE id=?", (str(message_id),))

        except sqlite3.Error as e:
            print("Error %s:" % e.args[0])
            return False


