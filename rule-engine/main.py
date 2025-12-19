from datetime import datetime, timezone
import os
import json
from datetime import datetime

import pika
from pymongo import MongoClient

import time
from pika.exceptions import AMQPConnectionError


# =========================
# Settings
# =========================
RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")
RABBIT_EXCHANGE = os.getenv("RABBIT_EXCHANGE", "iot_data")
QUEUE_NAME = os.getenv("RABBIT_QUEUE", "rule_engine_input")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "iot_port")
ALERTS_COLLECTION = os.getenv("ALERTS_COLLECTION", "alerts")

print("MONGO_URI =", MONGO_URI)
print("MONGO_DB =", MONGO_DB)
print("ALERTS_COLLECTION =", ALERTS_COLLECTION)

# =========================
# MongoDB
# =========================
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB]
alerts_col = db[ALERTS_COLLECTION]


# =========================
# Rules config (минимально)
# =========================
CRANE_OVERLOAD_THRESHOLD = 20.0          # тонн
OVERHEAT_THRESHOLD = 60.0                # °C
OVERHEAT_CONSECUTIVE_REQUIRED = 10       # пакетов подряд

# state для длящихся правил: по device_id считаем подряд "горячие" пакеты
overheat_streak_by_device: dict[int, int] = {}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_alert(rule_id: str, severity: str, message: str, telemetry: dict):
    doc = {
        "rule_id": rule_id,
        "severity": severity,
        "message": message,
        "device_id": telemetry.get("device_id"),
        "device_type": telemetry.get("device_type"),
        "location": telemetry.get("location"),
        "telemetry_timestamp": telemetry.get("timestamp"),
        "created_at": utc_now_iso(),
        "telemetry": telemetry,
    }

    try:
        res = alerts_col.insert_one(doc)
        print(
            f"ALERT INSERTED id={res.inserted_id} into {alerts_col.full_name}")
        print(f"ALERTS COUNT NOW: {alerts_col.count_documents({})}")
    except Exception as e:
        print(f"ERROR inserting alert into Mongo: {e}")
        raise


def apply_rules(telemetry: dict):
    """
    Возвращает список сработавших правил (для логирования).
    """
    triggered = []

    device_id = telemetry.get("device_id")
    device_type = telemetry.get("device_type")
    load_weight = telemetry.get("load_weight")
    temperature = telemetry.get("temperature")

    # Мгновенное правило: перегруз крана
    if device_type == "crane" and isinstance(load_weight, (int, float)) and load_weight > CRANE_OVERLOAD_THRESHOLD:
        rule_id = "CRANE_OVERLOAD"
        triggered.append(rule_id)
        create_alert(
            rule_id=rule_id,
            severity="HIGH",
            message=f"Crane overload: load_weight={load_weight}t > {CRANE_OVERLOAD_THRESHOLD}t",
            telemetry=telemetry
        )

    # Длящееся правило: перегрев 10 пакетов подряд
    if isinstance(device_id, int):
        if isinstance(temperature, (int, float)) and temperature > OVERHEAT_THRESHOLD:
            overheat_streak_by_device[device_id] = overheat_streak_by_device.get(
                device_id, 0) + 1
        else:
            overheat_streak_by_device[device_id] = 0

        if overheat_streak_by_device[device_id] >= OVERHEAT_CONSECUTIVE_REQUIRED:
            rule_id = "OVERHEAT_RISK_10"
            triggered.append(rule_id)
            create_alert(
                rule_id=rule_id,
                severity="MEDIUM",
                message=f"Overheat risk: temperature>{OVERHEAT_THRESHOLD}C for {OVERHEAT_CONSECUTIVE_REQUIRED} consecutive messages",
                telemetry=telemetry
            )
            # чтобы не спамить алёртами каждое следующее сообщение:
            overheat_streak_by_device[device_id] = 0

    return triggered


# =========================
# RabbitMQ connection
# =========================
def connect_rabbit_with_retry():
    delay = 2
    for attempt in range(1, 31):  # ~60 секунд
        try:
            print(
                f"[RabbitMQ] Connecting to {RABBIT_HOST} (attempt {attempt})...")
            conn = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBIT_HOST, heartbeat=60)
            )
            print("[RabbitMQ] Connected.")
            return conn
        except AMQPConnectionError as e:
            print(f"[RabbitMQ] Connection failed: {e}. Sleeping {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to RabbitMQ after multiple attempts")


connection = connect_rabbit_with_retry()
channel = connection.channel()


channel.exchange_declare(
    exchange=RABBIT_EXCHANGE,
    exchange_type="fanout",
    durable=True
)

channel.queue_declare(queue=QUEUE_NAME, durable=True)
channel.queue_bind(exchange=RABBIT_EXCHANGE, queue=QUEUE_NAME)

print("Rule Engine started. Waiting for messages...")


def on_message(ch, method, properties, body):
    try:
        telemetry = json.loads(body.decode("utf-8"))

        print("\n--- New message received ---")
        print(json.dumps(telemetry, indent=2))

        triggered = apply_rules(telemetry)

        if triggered:
            print(f"!!! RULES TRIGGERED: {triggered}")
        else:
            print("No rules triggered.")
            
        time.sleep(2)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:

        print(f"ERROR while processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


channel.basic_consume(
    queue=QUEUE_NAME,
    on_message_callback=on_message,
    auto_ack=False
)

channel.start_consuming()
