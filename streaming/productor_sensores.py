import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Configuración del productor Kafka
KAFKA_SERVER = "192.168.80.21:9092"
TOPIC = "sensor_data"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Productor de sensores iniciado. Presiona Ctrl+C para detenerlo.")

try:
    while True:
        dato = {
            "sensor_id": f"sensor_{random.randint(1, 3)}",
            "temperature": round(random.uniform(18.0, 35.0), 2),
            "humidity": round(random.uniform(30.0, 90.0), 2),
            "timestamp": datetime.now().isoformat()
        }

        producer.send(TOPIC, value=dato)
        producer.flush()

        print(f"Enviado: {dato}")
        time.sleep(2)

except KeyboardInterrupt:
    print("\nProductor detenido.")

finally:
    producer.close()
