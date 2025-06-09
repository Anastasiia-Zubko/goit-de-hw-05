from kafka import KafkaConsumer
from configs import kafka_config
import json

my_name = "anastasiia"
topics = [
    f"{my_name}_temperature_alerts",
    f"{my_name}_humidity_alerts"
]

consumer = KafkaConsumer(
    *topics,
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='alert_listener_group'
)

print(f"Listening for alerts on topics: {topics}")

for message in consumer:
    alert = message.value
    print(f"[{alert['type'].upper()} ALERT] Sensor {alert['sensor_id']} | Value: {alert['value']} | Time: {alert['timestamp']} | Msg: {alert['message']}")
