from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime
from pprint import pprint


# Генерація випадкових медичних даних
def generate_health_data(patient_id):
    return {
            "patient_id": patient_id,
            "heart_rate": random.randint(60, 100),
            "blood_pressure": f"{random.randint(110, 140)}/{random.randint(70, 90)}",
            "glucose_level": round(random.uniform(70, 140), 2),
            "timestamp": datetime.now().isoformat()
            }


# Відправка даних у Kafka
def send_data(iterations, producer):
    for i in range(iterations):
        data = generate_health_data(patient_id=i)
        producer.send("health-data", key=f"patient_{i}", value=data)
        pprint(f"Sent: key=patient_{i}, value={data}")
        time.sleep(1)

    producer.flush()
    producer.close()


if __name__ == "__main__":
    # Ініціалізація Kafka Producer
    producer = KafkaProducer(
                bootstrap_servers="localhost:29092",
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8")
            )

    send_data(100, producer)

