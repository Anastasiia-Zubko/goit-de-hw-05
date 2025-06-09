from kafka import KafkaProducer
from configs import kafka_config
import json, time, random, uuid

sensor_id = str(uuid.uuid4())
my_name = "anastasiia"
topic = f"{my_name}_building_sensors"

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

print(f"[START] Sensor ID: {sensor_id} sending to topic: {topic}")

try:
    for i in range(30):
        data = {
            "sensor_id": sensor_id,
            "timestamp": time.time(),
            "temperature": random.randint(25, 45),
            "humidity": random.randint(15, 85)
        }
        producer.send(topic, value=data)
        producer.flush()
        print(f"[{i+1}/30] Sent:", data)
        time.sleep(2)
except KeyboardInterrupt:
    print("Stopped by user.")
finally:
    producer.close()
    print("[DONE] Producer closed.")
