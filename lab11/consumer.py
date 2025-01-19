from kafka import KafkaConsumer
from pprint import pprint
import json


# Читання даних з топіка
def retrieve_data(consumer):
    for message in consumer:
        pprint(f"Received: key={message.key}, value={message.value}")


if __name__ == "__main__":
    # Ініціалізація Kafka Consumer
    consumer = KafkaConsumer(
            'health-data',
            bootstrap_servers='localhost:29092',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
        )

    retrieve_data(consumer)

