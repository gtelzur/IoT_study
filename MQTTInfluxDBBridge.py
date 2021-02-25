import paho.mqtt.client as mqtt
import time
from influxdb import InfluxDBClient


def on_message(client, userdata, msg):
    message=msg.payload.decode("utf-8")
    sensor_data = message[:-2]
    print(sensor_data)
    if sensor_data is not None:
      _send_sensor_data_to_influxdb(sensor_data)
    
def _send_sensor_data_to_influxdb(sensor_data):
    json_body = [
        {    
            'measurement': 'Temperature',
            'tags': {
                'location': 'office'
            },
            'fields': {
                'Temperaure': sensor_data
            }
        }
    ]
    influxdb_client.write_points(json_body)
    

     
def _init_influxdb_database():
    databases = influxdb_client.get_list_database()
    if len(list(filter(lambda x: x['name'] == INFLUXDB_DATABASE, databases))) == 0:
        influxdb_client.create_database(INFLUXDB_DATABASE)
    influxdb_client.switch_database(INFLUXDB_DATABASE)

INFLUXDB_ADDRESS = '192.168.1.184'
INFLUXDB_USER = 'mqtt'
INFLUXDB_PSSWORD = '62mqtt'
INFLUXDB_DATABASE = 'iot_station'    
MQTT_CLIENT_ID = 'MQTTInfluxDBBridge'
#influxdb_client = InfluxDBClient(INFLUXDB_ADDRESS, 8086, INFLUXDB_USER, INFLUXDB_PASSWORD, None)
influxdb_client = InfluxDBClient('192.168.1.184', 8086, 'mqtt', '62mqtt', None)
_init_influxdb_database() 

broker_address = '192.168.1.184'
client = mqtt.Client("P1")
print("connecting to broker")
rc=client.connect(broker_address)  # return code
client.subscribe("/home/office/temperature",qos=0)
client.on_message = on_message
time.sleep(2)
client.loop_forever()


