import paho.mqtt.client as mqtt

# Se define la dirección del broker
BROKER = "broker.emqx.io"  
PORT = 1883
KEEPALIVE = 60

# Confirmación de conexión
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Conectado exitosamente. Código de retorno: {rc}")
        # Suscribiéndose a los tópicos mediante wildcards
        client.subscribe("building/+/[piso]/+/+")
        client.subscribe("building/+/+/+/temperature")
        client.subscribe("alerts/+/+")
        client.subscribe("control/+/hvac/#")
        print("Suscrito a los tópicos exitosamente.")
    else:
        print(f"Error al conectar con el broker. Código de retorno: {rc}")
    
# Imprime el mensaje recibido
def on_message(client, userdata, msg):
    print(f"Mensaje recibido en el tópico {msg.topic}: {msg.payload.decode()}")

# Imprime mensajes retenidos
def on_publish(client, userdata, mid):
    print(f"Mensaje {mid} publicado.")

# Configuración del cliente MQTT
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_publish = on_publish

# Conexión al broker
client.connect(BROKER, PORT, KEEPALIVE)

# Publica las retain flags
client.publish("device/state", "active", retain=True)
client.publish("sensors/last_update", "2024-11-26T12:00:00Z", retain=True)
client.publish("system/configuration", '{"config": "default"}', retain=True)
client.publish("alert/values", '{"temperature": 75, "humidity": 45}', retain=True)

# Inicializa el loop MQTT
client.loop_forever()
