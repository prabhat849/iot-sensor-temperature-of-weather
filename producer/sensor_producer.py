import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

SENSOR_COUNT = 20
OUTPUT_FILE = Path("data/raw/sensor_readings.jsonl")


def generate_reading(sensor_number):
    # Occasionally simulate an unusually high temperature.
    if random.random() < 0.10:
        temperature = round(random.uniform(81, 95), 2)
    else:
        temperature = round(random.uniform(20, 80), 2)

    humidity = round(random.uniform(25, 85), 2)

    return {
        "sensor_id": f"SENSOR_{sensor_number:03d}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": temperature,
        "humidity": humidity,
        "machine_status": (
            "WARNING" if temperature > 80 else "NORMAL"
        ),
    }


def main():
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    print("IoT Sensor Data Generator Started")
    print(f"Number of sensors: {SENSOR_COUNT}")
    print(f"Output file: {OUTPUT_FILE}")
    print("Press Ctrl+C to stop.\n")

    try:
        with OUTPUT_FILE.open("a", encoding="utf-8") as file:
            while True:
                for number in range(1, SENSOR_COUNT + 1):
                    reading = generate_reading(number)

                    file.write(json.dumps(reading) + "\n")
                    file.flush()

                    print(json.dumps(reading))

                time.sleep(2)

    except KeyboardInterrupt:
        print("\nSensor data generation stopped.")


if __name__ == "__main__":
    main()
