from influxconnector.convertor.simple_json_dict import SimpleJSONDict
from influxconnector.client import InfluxClient
from ppmpmessage.convertor.ppmp import PPMPObject
from mqttconnector.client import MQTTClient
import time


MQTT_TOPIC = 'mh/ppmp'

if __name__ == "__main__":
    # connect to InfluxDB
    influx = InfluxClient()
    influx.connect()

    # connect to MQTT
    mqtt = MQTTClient()
    mqtt.connect()

    mqtt.subscribe(MQTT_TOPIC, lambda data: influx.write(PPMPObject(data).export_to_influxdb(), 'ppmp'))
    #mqtt.subscribe(MQTT_TOPIC, lambda data: influx.write(SimpleJSONDict(data).export_to_influxdb(), 'sofc'))

    while True:
        time.sleep(1)
