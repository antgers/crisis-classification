#! python3
from bus.message_listener import ListenerThread
from bus.incoming_messages_handler import IncomingMessagesHandler
import time


class CRCLService:
    def __init__(self, listen_to_topics):
        self.listen_to_topics = listen_to_topics

        # Initiate a thread to listen to the bus and put incoming messages to the database
        self.listener = ListenerThread(self.listen_to_topics)
        self.listener.setDaemon(True)
        self.listener.start()

        # Create a message handler object
        self.incoming_messages_handler = IncomingMessagesHandler()

        self.running = True

    def run_service(self):
        while self.running:
            # Process all messages in the database
            self.incoming_messages_handler.process_database_messages()

            # Sleep for a while
            time.sleep(0.72)

    def stop_service(self):
        # Stop listener thread
        self.listener.stop()

        # Stop message handling
        self.running = False
