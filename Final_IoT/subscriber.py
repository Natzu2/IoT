import random

from paho.mqtt import client as mqtt_client

# mediante una variable establemos el Broker al que queremos conectarnos
# ademas de una segunda variable para indicar el puerto de acceso
broker = 'broker.emqx.io'
port = 1883

# definimos los topicos a los que queremos subscribirnos
# asegurandonos de utilizar las wildcards necesarias para que el mensaje
# recibido sea valido para el topico
topic = "building/+/[piso]/+/+"
topic2 = "alerts/+/+"
topic3 = "control/+/hvac/#"

topic4 = "estado/+/[estado]"
topic5 = "sensor_critico/+/+"
topic6 = "configuracion/+/#"

# generamos un ID para cliente con un prefijo aleatorio
client_id = f'python-mqtt-{random.randint(0, 1000)}'
username = 'Enrique'
password = 'user13'

# creamos una funcion para establecer una conexion MQTT con el modulo mqtt_client
# de la libreria paho
def connect_mqtt() -> mqtt_client:

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

# creamos una funcion para subscribirse a los topicos usando la conexion del cliente
# y el modulo mqtt_client de la libreria paho
def subscribe(client: mqtt_client):
   def on_message(client, userdata, msg):
       print(f"From `{msg.topic}` topic Received `{msg.payload.decode()}` ")

   client.subscribe(topic)
   client.subscribe(topic2)
   client.subscribe(topic3)
   client.subscribe(topic4)
   client.subscribe(topic5)
   client.subscribe(topic6)
   client.on_message = on_message

# definimos una funcion para ejecutar la funcion para subscribirse a los topicos
def run():
   client = connect_mqtt()
   subscribe(client)
   client.loop_forever()

# ejecuta el codigo si es el punto de entrada
if __name__ == '__main__':
   run()
