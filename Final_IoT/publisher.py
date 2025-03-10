import random
import time

from datetime import datetime

from paho.mqtt import client as mqtt_client
import time

# mediante una variable establemos el Broker al que queremos conectarnos
# ademas de una segunda variable para indicar el puerto de acceso
broker = 'broker.emqx.io'
port = 1883

# definimos los topicos en los que queremos publicar mensajes
# y la estructura de estos
topic = "building/[edificio]/[piso]/[habitacion]/[tipo_sensor]"
topic2 = "building/[edificio]/[piso]/[habitacion]/[temperatura]"
topic3 = "alerts/[edificio]/[tipo]"
topic4 = "control/[edificio]/hvac/[dispositivo]"

topic5 = "estado/[dispositivo]/[estado]"
topic6 = "sensor_critico/[lectura]/[nivel]"
topic7 = "configuracion/[tipo]"


# generamos un ID para cliente con un prefijo aleatorio
client_id = f'python-mqtt-{random.randint(0, 1000)}'
username = 'Enrique'
password = 'user13'

# creamos una funcion para establecer una conexion MQTT con el modulo mqtt_client
# de la libreria paho
def connect_mqtt():

    # creamos una funcion para indicar si la conexion se establecio correctamente
    # o que indique si hubo un probela al realizar la conexion
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    # establecemos conexion mediante mqtt_client.CallbackAPIVersion y lo relacionamos con el cliente genrado
    # anteriormente y solicitar que retorne la conexion con el Broker
    client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1, client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

# creamos una funcion para publicar mensajes en la conexion "client" 
def publish(client):

    # mientras no este recibiendo mensajes empieza un ciclo
    msg_count = 0
    while True:

        # sensores simulados
        sensor = 40
        sensor_critico = 150

        # mensajes a publicar
        msg2 = fecha()
        msg = f"CEEDER/2/911/temperatura " + f'hora {msg2}'
        msg1 = f"CEEDER/2/911/{sensor}°C " + f'hora {msg2}'
        msg3 = f"CEEDER/alarma/alerta/Peligro " + f'hora {msg2}'
        msg4 = f"CEEDER/hvac/temperatura/{sensor}°C " + f'hora {msg2}'

        msg5 = f'Sensor termico/funcionando ' + f'hora {msg2}'
        msg6 = f'{sensor_critico}°C/nivel: critico ' + f'hora {msg2}'
        msg7 = f'Temperatura > 90°C/apague el sistema/tome medidas de seguridad ' + f'hora {msg2}'

        # se publica el mensaje y se relaciona al topico con el que esta
        # estructurado
        result = client.publish(topic, msg, retain=True)
        result1 = client.publish(topic2, msg1, retain=True)
        result2 = client.publish(topic3, msg3, retain=True)
        result3 = client.publish(topic4, msg4, retain=True)

        result4 = client.publish(topic5, msg5, retain=True)
        result5 = client.publish(topic6, msg6, retain=True)
        result6 = client.publish(topic7, msg7, retain=False)

        status = result[0]

        # indicamos una condición, si no existe mensaje ya publicado entonces publica los mensajes
        # en caso de algun problema indicara el topico donde fue imposible publicar el mensaje
        if status == 0:
            print(f"Send `{msg}` to topic `{topic}`")
            print(f"Send `{msg1}` to topic `{topic2}`")
            print(f"Send `{msg3}` to topic `{topic3}`")
            print(f"Send `{msg4}` to topic `{topic4}`")

            print(f"Send `{msg5}` to topic `{topic5}`")
            print(f"Send `{msg6}` to topic `{topic6}`")
            print(f"Send `{msg7}` to topic `{topic7}`")
        else:
            print(f"Failed to send message to topic {topic}")

        # se publican los mensajes y se indica con time.sleep cuantos segundos
        # debe esperar antes de publicar mensajes nuevos
        msg_count += 1
        time.sleep(20)

# creamos una funcion para ejecutar la funcion publish en la conexion del cliente
# y se indica que al iniciar comience un ciclo
def run():
    client = connect_mqtt()
    client.loop_start()
    publish(client)

# creamos una funcion fecha para darle formato a la fecha y añadir la fecha de 
# publicacion de los mensajes
def fecha():
    now = datetime.now()
    fecha_hora = now.strftime("%d/%m/%Y %H:%M:%S")
    return fecha_hora

# ejecuta el codigo si es el punto de entrada
if __name__ == '__main__':
    run()