import unittest
from unittest.mock import MagicMock, call
import paho.mqtt.client as mqtt

class MQTTClient:
    def __init__(self, broker, port, keepalive):
        self.client = mqtt.Client()
        self.broker = broker
        self.port = port
        self.keepalive = keepalive
        self.configure_callbacks()

    def configure_callbacks(self):
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_publish = self.on_publish

    def on_connect(self, client, userdata, flags, rc):
        print(f"Connected with result code {rc}")
        client.subscribe("building/+/[piso]/+/+")
        client.subscribe("building/+/+/+/temperature")
        client.subscribe("alerts/+/+")
        client.subscribe("control/+/hvac/#")
        print("Subscrito a los topicos.")

    def on_message(self, client, userdata, msg):
        print(f"Mensage recivido en el topico {msg.topic}: {msg.payload.decode()}")

    def on_publish(self, client, userdata, mid):
        print(f"Mensaje {mid} publicado.")

    def publish_retained_messages(self):
        self.client.publish("device/state", "active", retain=True)
        self.client.publish("sensors/last_update", "2024-11-26T12:00:00Z", retain=True)
        self.client.publish("system/configuration", '{"config": "default"}', retain=True)
        self.client.publish("alert/values", '{"temperature": 75, "humidity": 45}', retain=True)

    def start(self):
        self.client.connect(self.broker, self.port, self.keepalive)
        self.publish_retained_messages()
        self.client.loop_forever()


# Unit test
class TestMQTTClient(unittest.TestCase):
    def setUp(self):
        self.broker = "test.mosquitto.org"
        self.port = 1883
        self.keepalive = 60
        self.client = MQTTClient(self.broker, self.port, self.keepalive)
        self.client.client.connect = MagicMock()
        self.client.client.publish = MagicMock()
        self.client.client.subscribe = MagicMock()

    def test_connection(self):
        self.client.client.connect(self.broker, self.port, self.keepalive)
        self.client.client.connect.assert_called_once_with(self.broker, self.port, self.keepalive)

    def test_subscription(self):
        self.client.on_connect(self.client.client, None, None, 0)
        expected_calls = [
            call("building/+/[piso]/+/+"),
            call("building/+/+/+/temperature"),
            call("alerts/+/+"),
            call("control/+/hvac/#"),
        ]
        self.client.client.subscribe.assert_has_calls(expected_calls, any_order=True)

    def test_publish_retained_messages(self):
        self.client.publish_retained_messages()
        expected_calls = [
            call("device/state", "active", retain=True),
            call("sensors/last_update", "2024-11-26T12:00:00Z", retain=True),
            call("system/configuration", '{"config": "default"}', retain=True),
            call("alert/values", '{"temperature": 75, "humidity": 45}', retain=True),
        ]
        self.client.client.publish.assert_has_calls(expected_calls, any_order=True)


if __name__ == "__main__":
    unittest.main()
