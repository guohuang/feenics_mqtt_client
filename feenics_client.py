import json
import random

import bson
import requests
from paho.mqtt import client as mqtt_client


class FeenicsClient:
    def __init__(self, log, **kwargs):
        self.topic = None
        self.token = None
        self._on_save_event = None
        self.log = log
        self.auth_url = kwargs["auth"]["auth_url"]
        self.instance_name = kwargs["auth"]["instance_name"]
        self.username = kwargs["auth"]["username"]
        self.password = kwargs["auth"]["password"]

    @property
    def on_save_event(self):
        """save event delegation"""

        return self._on_save_event

    @on_save_event.setter
    def on_save_event(self, func):
        """
        delegate function to output this event

        """
        self._on_save_event = func

    # Login into Feenics and Return Token & Instance Key
    def get_auth_token_and_instance_id(self):
        LoginHeader = {'Content-Type': 'application/x-www-form-urlencoded'}
        LoginData = {'instance': self.instance_name, 'username': self.username, 'password': self.password}
        LoginRequest = requests.post(self.auth_url, headers=LoginHeader, data=LoginData)
        LoginRequestResponse = json.loads(LoginRequest.text)

        self.token = LoginRequestResponse['access_token']
        self.topic = f"/{LoginRequestResponse['instance']}/$"

    def connect_mqtt(self, broker, port, mqtt_path):
        self.get_auth_token_and_instance_id()
        client_id = f'python-mqtt-{random.randint(0, 100)}'
        client = mqtt_client.Client(client_id, transport="websockets")
        client.ws_set_options(path=mqtt_path, headers=None)
        client.username_pw_set(self.token)
        client.tls_set()
        client.on_connect = self.on_connect
        client.on_message = self.on_message
        client.on_log = self.on_log
        client.connect(broker, port)

        return client

    def on_log(self, client, obj, level, string):
        self.log.debug(string)

    def on_connect(self, client, userdata, flags, rc):
        ''' MQTT return code reference:
            0: Connection successful.
            1: Connection refused – incorrect protocol version.
            2: Connection refused – invalid client identifier.
            3: Connection refused – server unavailable.
            4: Connection refused – bad username or password.
            5: Connection refused – not authorised.
            6-255: Currently unused.
        '''

        if rc == 0:
            self.log.info("Connected to MQTT Broker!")
            # Subscribing in on_connect() means that if we lose the connection and
            # reconnect then subscriptions will be renewed.
            client.subscribe(self.topic)
        elif rc == 5:
            self.log.warning("Connection refused – not authorised, token expired. Return code %d\n", rc)
            self.get_auth_token_and_instance_id()
            client.username_pw_set(self.token)
            client.reconnect()
        else:
            self.log.error("Failed to connect, return code %d\n", rc)

    def on_message(self, client, userdata, msg):
        self.log.info("New events received.")
        message = bson.loads(msg.payload)
        self._on_save_event(message)
