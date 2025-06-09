from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config
import json

my_name = "anastasiia"

consumer = KafkaConsumer(
    f"{my_name}_building_sensors",
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='alert_processor_group'
)

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for msg in consumer:
    data = msg.value
    alerts = []

    if data['temperature'] > 40:
        alerts.append({
            "type": "temperature",
            "sensor_id": data['sensor_id'],
            "timestamp": data['timestamp'],
            "value": data['temperature'],
            "message": "Температура перевищує 40°C"
        })

    if data['humidity'] < 20 or data['humidity'] > 80:
        alerts.append({
            "type": "humidity",
            "sensor_id": data['sensor_id'],
            "timestamp": data['timestamp'],
            "value": data['humidity'],
            "message": "Вологість поза межами 20-80%"
        })

    for alert in alerts:
        topic = f"{my_name}_{alert['type']}_alerts"
        producer.send(topic, value=alert)
        print("Alert sent to", topic, ":", alert)

producer.close()
