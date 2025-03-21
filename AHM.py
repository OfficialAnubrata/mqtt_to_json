import paho.mqtt.client as mqtt
import json

# MQTT Broker details
BROKER_1 = "3.230.164.113"
BROKER_2 = "3.230.164.113"
PORT = 1883

# Authentication for second broker
USERNAME = "IEMA@2024"
PASSWORD = "Pass@IEMA2024"

DEVICE_COUNT = 15

# Device-specific topics
DEVICE_TOPICS = {
    f"IEMAAHM00{i + 1}": [
        f"/ahm00{i + 1}/mic1",
        f"/ahm00{i + 1}/mic2",
        f"/ahm00{i + 1}/accelx",
        f"/ahm00{i + 1}/accely",
        f"/ahm00{i + 1}/accelz",
        f"/ahm00{i + 1}/gyrox",
        f"/ahm00{i + 1}/gyroy",
        f"/ahm00{i + 1}/gyroz",
        f"/ahm00{i + 1}/magx",
        f"/ahm00{i + 1}/magy",
        f"/ahm00{i + 1}/magz"
    ] for i in range(DEVICE_COUNT)
}

# Data storage for each device
sensor_data = {
    f"IEMAAHM00{i + 1}": {
        "device_id": f"IEMAAHM00{i + 1}",
        "temperature_one": "28.7",
        "temperature_two": "28.7",
        "vibration_x": "0.00",
        "vibration_y": "0.00",
        "vibration_z": "0.00",
        "gyro_x": "0.00",
        "gyro_y": "0.00",
        "gyro_z": "0.00",
        "magnetic_flux_x": "0.00",
        "magnetic_flux_y": "0.00",
        "magnetic_flux_z": "0.00",
        "ultra_sound": "52.1",  # Placeholder
        "audible_sound": "34.04"  # Placeholder
    } for i in range(DEVICE_COUNT)
}

# Mapping topics to JSON keys dynamically
mqtt_to_json_keys = {}
for i in range(DEVICE_COUNT):
    mqtt_to_json_keys.update({
        f"/ahm00{i + 1}/mic1": "ultra_sound",
        f"/ahm00{i + 1}/mic2": "audible_sound",
        f"/ahm00{i + 1}/accelx": "vibration_x",
        f"/ahm00{i + 1}/accely": "vibration_y",
        f"/ahm00{i + 1}/accelz": "vibration_z",
        f"/ahm00{i + 1}/gyrox": "gyro_x",
        f"/ahm00{i + 1}/gyroy": "gyro_y",
        f"/ahm00{i + 1}/gyroz": "gyro_z",
        f"/ahm00{i + 1}/magx": "magnetic_flux_x",
        f"/ahm00{i + 1}/magy": "magnetic_flux_y",
        f"/ahm00{i + 1}/magz": "magnetic_flux_z"
    })


def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    for device, topics in DEVICE_TOPICS.items():
        for topic in topics:
            client.subscribe(topic)
    print("Subscribed to topics for all devices")


def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode()
    print(f"Received message from {topic}: {payload}")

    for device, topics in DEVICE_TOPICS.items():
        if topic in topics:
            sensor_data[device][mqtt_to_json_keys[topic]] = payload
            json_payload = json.dumps(sensor_data[device])

            # Publish to both brokers
            client.publish(f"{device}", json_payload)
            print(f"Published data to {device}: {json_payload}")

            client2.publish(f"{device}", json_payload)
            print(f"Published data to second broker {BROKER_2}: {json_payload}")

            break


# MQTT client setup for first broker
client = mqtt.Client()
client.username_pw_set(USERNAME, PASSWORD)
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER_1, PORT, 60)

# MQTT client setup for second broker with authentication
client2 = mqtt.Client()
client2.username_pw_set(USERNAME, PASSWORD)
client2.connect(BROKER_2, PORT, 60)

client.loop_forever()