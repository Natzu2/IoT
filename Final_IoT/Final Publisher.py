import random
import time

from datetime import datetime

from paho.mqtt import client as mqtt_client
import time

broker = 'broker.emqx.io'
port = 1883

topic = "building/[edificio]/[piso]/[habitación]/[tipo_sensor]"
topic1 = "control/[edificio]/[sistema]/[dispositivo]"
topic2 = "alerts/[edificio]/[tipo]"
topic3 = "$SYS/broke.emqx.io/uptime"

# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 1000)}'
username = f'PROFESOR3df3-{random.randint(0, 1000)}'
password = 'admindf'

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

def publish(client):
    msg_count = 0
    while True:

        #msg = f"messages:{RFC}-{msg_count}"
        msg = f"[2,3,5,34,45,3,5,3,4,5,6,3,3,56,34,4,3,3,4,5,3,5,34,3,23,2,6,]"
        msg2 = fecha()
        msg3 = f"{msg}{msg2}"
        result = client.publish(topic, msg3, retain=True)
        # result: [0, 1]
        status = result[0]
        if status == 0:
            print(f"Send `{msg}` hora '{msg2}' to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")
        msg_count += 1
        time.sleep(180)


def run():
    client = connect_mqtt()
    client.loop_start()
    """This is part of the threaded client interface. Call this once to
    start a new thread to process network traffic. This provides an
    alternative to repeatedly calling loop() yourself.
    """
    publish(client)


def fecha():
    now = datetime.now()
    fecha_hora = now.strftime("%d/%m/%Y %H:%M:%S")
    return fecha_hora

if __name__ == '__main__':
    run()
    #print(fecha())
