import random
import time

from datetime import datetime

from paho.mqtt import client as mqtt_client
import time

broker = 'broker.emqx.io'
port = 1883

topic = "building/[edificio]/[piso]/[habitacion]/[tipo_sensor]"
topic2 = "building/[edificio]/[piso]/[habitacion]/[temperatura]"
topic3 = "alerts/[edificio]/[tipo]"
topic4 = "control/[edificio]/hvac/[dispositivo]"

# topic1 = "estado/[dispositivo]/[estado]"
# topic2 = "sensor_critico/[lectura]/[fecha]"
# topic3 = "configuracion/[edificio]/hvac/[dispositivo]"
# topic4 = "alerts/[edificio]/[tipo]"


# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 1000)}'
username = 'Enrique'
password = 'user13'

def connect_mqtt():
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

def publish(client):
    msg_count = 0
    while True:

        sistema = "hvac"

        msg = f"CEEDER/2/911/temperatura "
        msg1 = f"CEEDER/2/911/80°C "
        msg2 = fecha()
        msg3 = f"{msg}{msg2}"
        msg4 = f"CEEDER/alarma/alerta/Peligro "
        msg5 = f"CEEDER/hvac/temperatura/0°C "
        msg6 = f"{msg4}{msg2}"
        msg7 = f"{msg5}{msg2}"
        result = client.publish(topic, msg3, retain=False)
        result1 = client.publish(topic2, msg1, retain=False)
        result2 = client.publish(topic3, msg6, retain=False)
        result3 = client.publish(topic4, msg7, retain=False)

        status = result[0]
        if status == 0:
            print(f"Send `{msg}` hora '{msg2}' to topic `{topic}`")
            print(f"Send `{msg1}` hora '{msg2}' to topic `{topic2}`")
            print(f"Send `{msg4}` hora '{msg2}' to topic `{topic3}`")
            print(f"Send `{msg5}` hora '{msg2}' to topic `{topic4}`")
        else:
            print(f"Failed to send message to topic {topic}")
        msg_count += 1
        time.sleep(30)


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