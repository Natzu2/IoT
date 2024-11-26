# python3.6

import random

from paho.mqtt import client as mqtt_client


broker = 'broker.emqx.io'
port = 1883

topic = "building/+/[piso]/+/+"
topic2 = "alerts/+/+"
topic3 = "control/+/hvac/#"

# topic = "estado/+/[estado]"
# topic2 = "sensor_critico/+/+"
# topic3 = "control/+/hvac/#"

# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 1000)}'
username = 'Enrique'
password = 'user13'


def connect_mqtt() -> mqtt_client:
   def on_connect(client, userdata, flags, rc):
       if rc == 0:
           print("Connected to MQTT Broker!")
       else:
           print("Failed to connect, return code %d\n", rc)

   client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1, client_id)
   client.username_pw_set(username, password)
   client.on_connect = on_connect
   client.connect(broker, port)
   return client


def subscribe(client: mqtt_client):
   def on_message(client, userdata, msg):
       print(f"From `{msg.topic}` topic Received `{msg.payload.decode()}` ")

   client.subscribe(topic)
   client.subscribe(topic2)
   client.subscribe(topic3)
   client.on_message = on_message


def run():
   client = connect_mqtt()
   subscribe(client)
   client.loop_forever()


if __name__ == '__main__':
   run()
