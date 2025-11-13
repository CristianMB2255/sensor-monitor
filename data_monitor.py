import paho.mqtt.client as paho
from paho import mqtt
import time
import json
import os
import psycopg
from dotenv import load_dotenv

load_dotenv()
conn_string = os.getenv("DATABASE_URL")
mqtt_username = os.getenv("USERNAME")
mqtt_password = os.getenv("PASSWORD")
mqtt_host = os.getenv("HOST")

def connect_db():
    return psycopg.connect(conn_string)

conn = connect_db()

def insert_db(timestamp, temperature, humidity):
    try:
        with conn.cursor() as cur:
            print("Connection established")
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO data (timestamp, temperature, humidity) VALUES (%s, %s, %s);",
                    (timestamp, temperature, humidity),
                )
                print("Value inserted.")
    except Exception as e:
        print("Connection failed.")
        print(e)

def on_connect(client, userdata, flags, rc, properties=None):
    print("CONNACK received with code %s." % rc)

def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))

def on_message(client, userdata, msg):
    local_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    payload = msg.payload.decode()

    print("Time: " + local_time + "\nPayload: " + payload)

    try:
        data = json.loads(payload)
        temp = data.get("temp")
        hum = data.get("hum")

        insert_db(local_time, temp, hum)

    except json.JSONDecodeError:
        print("Payload is not valid JSON.")


client = paho.Client(client_id="", userdata=None, protocol=paho.MQTTv5)
client.on_connect = on_connect

client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)
client.username_pw_set(mqtt_username, mqtt_password)
client.connect(mqtt_host, 8883)

client.on_subscribe = on_subscribe
client.on_message = on_message

client.subscribe("casa/sensor/#", qos=1)

try: 
    client.loop_forever()
except KeyboardInterrupt:
    print("Disconnected by user")
except Exception as exception:
    print("Error in loop:", exception)