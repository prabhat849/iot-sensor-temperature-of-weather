import json
import time
from pathlib import Path
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC = "iot-sensor-data"
INPUT_FILE = Path("data/raw/sensor_readings.jsonl")


def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Sensor data file not found: {INPUT_FILE}"
        )

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=3,
    )

    sent = 0

    try:
        with INPUT_FILE.open("r", encoding="utf-8") as file:
            for line in file:
                if not line.strip():
                    continue

                reading = json.loads(line)
                metadata = producer.send(TOPIC, value=reading).get(
                    timeout=30
                )
                sent += 1

                print(
                    f"Sent {sent}: sensor={reading['sensor_id']}, "
                    f"partition={metadata.partition}, "
                    f"offset={metadata.offset}"
                )

                time.sleep(0.05)

        producer.flush()
        print(f"\nCompleted. Total records sent: {sent}")

    finally:
        producer.close()


if __name__ == "__main__":
    main()
