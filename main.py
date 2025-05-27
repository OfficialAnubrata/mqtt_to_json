import paho.mqtt.client as mqtt
import json
import time
from collections import defaultdict

# MQTT Broker Settings
BROKER_1 = "3.230.164.113"
BROKER_2 = "3.230.164.113"
PORT     = 1883
USERNAME = "IEMA@2024"
PASSWORD = "Pass@IEMA2024"

# How many DUO devices?
DEVICE_COUNT = 300

# Delay between sending combined data (in seconds)
SEND_DELAY_SECONDS = 5

# Sensor keys expected per device
SENSOR_KEYS = [
    "temperature_one", "temperature_two",
    "vibration_x", "vibration_y", "vibration_z",
    "gyro_x", "gyro_y", "gyro_z",
    "magnetic_flux_x", "magnetic_flux_y", "magnetic_flux_z"
]

# Generate device topic map
DEVICE_IDS = [f"IEMA-601-2-00{i+1}" for i in range(DEVICE_COUNT)]
DEVICE_TOPICS = {
    device_id: [f"{device_id}/{key}" for key in SENSOR_KEYS]
    for device_id in DEVICE_IDS
}

# Reverse mapping: topic → (device_id, key)
TOPIC_TO_KEY = {}
for device_id, topics in DEVICE_TOPICS.items():
    for topic in topics:
        key = topic.split("/")[-1]
        TOPIC_TO_KEY[topic] = (device_id, key)

# Storage for sensor data
sensor_data = {
    device_id: {
        "device_id": device_id,
        **{key: "0.0" for key in SENSOR_KEYS}
    } for device_id in DEVICE_IDS
}

# Last publish timestamp
last_publish_time = defaultdict(lambda: 0)

def on_connect(client, userdata, flags, rc):
    print(f"✅ Connected with result code {rc}")
    for topic in TOPIC_TO_KEY:
        client.subscribe(topic)
    print("📡 Subscribed to all DUO device topics.")

def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode()

    if topic in TOPIC_TO_KEY:
        device_id, key = TOPIC_TO_KEY[topic]
        sensor_data[device_id][key] = payload
        print(f"📥 {device_id} | {key}: {payload}")

        current_time = time.time()
        if current_time - last_publish_time[device_id] >= SEND_DELAY_SECONDS:
            json_payload = json.dumps(sensor_data[device_id])
            client.publish(device_id, json_payload)
            client2.publish(device_id, json_payload)
            print(f"🚀 Published to {device_id}: {json_payload}")
            last_publish_time[device_id] = current_time

# Setup Client 1
client = mqtt.Client()
client.username_pw_set(USERNAME, PASSWORD)
client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER_1, PORT, 60)

# Setup Client 2
client2 = mqtt.Client()
client2.username_pw_set(USERNAME, PASSWORD)
client2.connect(BROKER_2, PORT, 60)

# Start loop
client.loop_forever()
