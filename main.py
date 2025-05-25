import os
import json
import base64
from typing import Union
from fastapi import FastAPI
import firebase_admin
from firebase_admin import credentials, db
import paho.mqtt.client as mqtt
import threading
import time
from datetime import datetime, timedelta
from pydantic import BaseModel
from fastapi import Query

# Decode the service account credentials from the environment variable
service_account_info = json.loads(base64.b64decode(os.environ['GOOGLE_CREDENTIALS_BASE64']))

# Initialize Firebase
cred = credentials.Certificate(service_account_info)
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://growmush-ce1f0-default-rtdb.asia-southeast1.firebasedatabase.app'
})

class Item(BaseModel):
    type: str
    status: bool

class St(BaseModel):
    status: bool
    
# FastAPI app
app = FastAPI()

@app.get("/")
def read_root():
    # //get paramiter
    return {"Hello": "World"}

@app.post("/control")
def control_status(item: Item):
    type = item.type
    if type not in ["fanStatus", "misterStatus", "lightStatus"]:
        return {"status": "error", "message": "Invalid type. Use 'fanStatus' or 'misterStatus'."}
    status = item.status
    if(status != True and status != False):
        return {"status": "error", "message": "Invalid status. Use 'True' or 'False'."}
    else :
        print(f"Fan status updated to: {status}")

    if(status == True or status == False):
        # Save to Firebase
        ref = db.reference("/"+type)
        ref.set(status)
        # Publish to MQTT
        if(status == True):
            s = "ON"
        else:
            s = "OFF"
        send_status(s, type)

    return {"status": "success", "state": status}

@app.post("/contorol_AI_mode")
def contorol_AI_mode(item: St):
    status = item.status
    if(status != True and status != False):
        return {"status": "error", "message": "Invalid status. Use 'True' or 'False'."}
    else:
        print(f"AI mode updated to: {status}")

    # Save to Firebase
    ref = db.reference("/AI_mode")
    ref.set(status)

    return {"status": "success", "state": status}

@app.get("/sensor_history")
def get_temperature_history(
        type: str = Query("temp", description="Sensor type: temp, humidity, or lightIntensity"),
        date: str = Query(None, description="Date in YYYY-MM-DD format (optional, defaults to today)")
    ):
    ref = db.reference("/"+type)
    data = ref.get()
    if not data:
        return {"status": "error", "message": "No data found."}

    # Convert data to list of (timestamp, value) tuples
    records = []
    for key, entry in data.items():
        try:
            ts = float(entry.get("timestamp", 0))
            val = float(entry.get("value", 0))
            records.append((ts, val))
        except Exception:
            continue

    # Filter records by date if provided
    if date:
        try:
            day_start = datetime.strptime(date, "%Y-%m-%d")
        except Exception:
            return {"status": "error", "message": "Invalid date format. Use YYYY-MM-DD."}
    else:
        day_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)
    start_ts = day_start.timestamp()
    end_ts = day_end.timestamp()
    records = [(t, v) for t, v in records if start_ts <= t < end_ts]

    # Sort by timestamp in descending order
    records.sort(key=lambda x: x[0], reverse=True)
    logs = [
        {
            "datetime": datetime.fromtimestamp(t).strftime("%Y-%m-%d %H:%M:%S"),
            "value": v
        }
        for t, v in records
    ]

    # Group into 15min intervals and calculate average
    result = []
    interval = 120 * 60  # 15 minutes in seconds
    if records:
        current = start_ts
        while current < end_ts:
            group = [v for t, v in records if current <= t < current + interval]
            if group:
                avg = sum(group) / len(group)
                dt = datetime.fromtimestamp(current)
                result.append({
                    "datetime": dt.strftime("%H:%M"),
                    "average_value": avg
                })
            current += interval

    return {"status": "success", "data": result, "logs": logs}

# MQTT Config
mqtt_broker = "904071d8450444f2a510827a43e54df4.s1.eu.hivemq.cloud"
mqtt_port = 8883
mqtt_user = "MushroomHouse"
mqtt_password = "Pathum123"

topics = [
    "mushroomHouse/temp",
    "mushroomHouse/humidity",
    "mushroomHouse/lightIntensity",
    "mushroomHouse/tempout",
    "mushroomHouse/humout"
]
client = mqtt.Client()

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    for topic in topics:
        client.subscribe(topic)
        print(f"Subscribed to {topic}")

def on_message(client, userdata, msg):
    print(f"{msg.topic} -> {msg.payload.decode()}")
    temp = msg.payload.decode()
    if temp == "nan":
        value = float(0)
    else:
        value = float(temp)

    payload = {
        "value": value,
        "timestamp": time.time()
    }
    # Save to Firebase by adding a new row (push)
    ref = db.reference(f"/{msg.topic.split('/')[-1]}")
    ref.push(payload)

    nref = db.reference(f"/live/{msg.topic.split('/')[-1]}/")
    nref.set(value)


def mqtt_loop():
    global client
    client.username_pw_set(mqtt_user, mqtt_password)
    client.tls_set()  # Use default SSL/TLS
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(mqtt_broker, mqtt_port, 60)
    client.loop_forever()

def send_status(status: str, type: str = "fanStatus"):
    global client
    topic = "mushroomHouse/"+type
    client.publish(topic, status)
    print(f"Published {status} to {topic}")


# Start MQTT in background thread
threading.Thread(target=mqtt_loop, daemon=True).start()
