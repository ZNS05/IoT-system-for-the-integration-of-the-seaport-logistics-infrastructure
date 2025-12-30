from datetime import datetime, timezone
import os
import json
import time

import pika
from pika.exceptions import AMQPConnectionError
from pymongo import MongoClient

import redis
from redis.exceptions import RedisError

from prometheus_client import start_http_server, Counter, Histogram, Gauge


# =========================
# Settings
# =========================
RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")
RABBIT_EXCHANGE = os.getenv("RABBIT_EXCHANGE", "iot_data")
QUEUE_NAME = os.getenv("RABBIT_QUEUE", "rule_engine_input")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "iot_port")
ALERTS_COLLECTION = os.getenv("ALERTS_COLLECTION", "alerts")

METRICS_PORT = int(os.getenv("METRICS_PORT", "9101"))

# Redis (для масштабирования state)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_STREAK_TTL_SEC = int(os.getenv("REDIS_STREAK_TTL_SEC", "3600"))

print("MONGO_URI =", MONGO_URI)
print("MONGO_DB =", MONGO_DB)
print("ALERTS_COLLECTION =", ALERTS_COLLECTION)
print("REDIS =", f"{REDIS_HOST}:{REDIS_PORT}")


# =========================
# MongoDB
# =========================
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB]
alerts_col = db[ALERTS_COLLECTION]


# =========================
# Redis
# =========================
redis_client = redis.Redis(
    host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
try:
    redis_client.ping()
    print(f"[Redis] Connected to {REDIS_HOST}:{REDIS_PORT}")
except RedisError as e:
    # Не падаем сразу, но будет видно по логам — правила по streak не будут работать корректно
    print(f"[Redis] ERROR: cannot connect to Redis: {e}")


# =========================
# Prometheus metrics
# =========================
MESSAGES_TOTAL = Counter(
    "rule_engine_messages_total",
    "Total messages processed by rule-engine",
    ["status"]
)

RULE_TRIGGERED_TOTAL = Counter(
    "rule_engine_rule_triggered_total",
    "How many times a rule was triggered",
    ["rule_id", "severity"]
)

PROCESSING_SECONDS = Histogram(
    "rule_engine_processing_seconds",
    "Time spent processing one message"
)

OVERHEAT_STREAK = Gauge(
    "rule_engine_overheat_streak",
    "Current overheat streak per device_id",
    ["device_id"]
)

telemetry_messages_processed_total = Counter(
    "telemetry_messages_processed_total",
    "Total number of telemetry messages processed by rule-engine"
)

alerts_created_total = Counter(
    "alerts_created_total",
    "Total number of alerts created by rule-engine"
)

rules_triggered_total = Counter(
    "rules_triggered_total",
    "Total number of triggered rules by rule-engine",
    ["rule_id"]
)

telemetry_processing_seconds = Histogram(
    "telemetry_processing_seconds",
    "Time spent processing a telemetry message in rule-engine"
)


# =========================
# Rules config
# =========================
CRANE_OVERLOAD_THRESHOLD = 20.0          # тонн
OVERHEAT_THRESHOLD = 60.0               # °C
OVERHEAT_CONSECUTIVE_REQUIRED = 10      # пакетов подряд


def log_json(service: str, level: str, event: str, **fields):
    payload = {
        "ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "service": service,
        "level": level,
        "event": event,
        **fields
    }
    print(json.dumps(payload, ensure_ascii=False))


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
        alerts_created_total.inc()
        print(
            f"ALERT INSERTED id={res.inserted_id} into {alerts_col.full_name}")
        print(f"ALERTS COUNT NOW: {alerts_col.count_documents({})}")
    except Exception as e:
        print(f"ERROR inserting alert into Mongo: {e}")
        raise


def to_float(x):
    """Пытаемся привести к float (поддерживает числа и строки)."""
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        try:
            return float(x)
        except ValueError:
            return None
    try:
        return float(x)
    except Exception:
        return None


def to_int(x):
    if isinstance(x, int):
        return x
    if isinstance(x, str) and x.isdigit():
        return int(x)
    return None


def _streak_key(device_id: int) -> str:
    return f"overheat_streak:{device_id}"


def apply_rules(telemetry: dict):
    triggered = []

    device_id_raw = telemetry.get("device_id")
    device_id = to_int(device_id_raw)
    device_type = telemetry.get("device_type")

    load_weight = to_float(telemetry.get("load_weight"))
    temperature = to_float(telemetry.get("temperature"))

    # 1) CRANE_OVERLOAD
    if device_type == "crane" and load_weight is not None and load_weight > CRANE_OVERLOAD_THRESHOLD:
        rule_id = "CRANE_OVERLOAD"
        triggered.append(rule_id)

        rules_triggered_total.labels(rule_id=rule_id).inc()
        RULE_TRIGGERED_TOTAL.labels(rule_id=rule_id, severity="HIGH").inc()

        log_json(
            "rule-engine", "WARN", "RULE_TRIGGERED",
            rule_id=rule_id,
            device_id=device_id_raw,
            device_type=device_type,
            load_weight=load_weight
        )

        create_alert(
            rule_id=rule_id,
            severity="HIGH",
            message=f"Crane overload: load_weight={load_weight}t > {CRANE_OVERLOAD_THRESHOLD}t",
            telemetry=telemetry
        )

    # 2) OVERHEAT_RISK_10 (10 подряд) — state в Redis
    if device_id is not None:
        streak = 0
        key = _streak_key(device_id)

        try:
            if temperature is not None and temperature > OVERHEAT_THRESHOLD:
                streak = int(redis_client.incr(key))
                redis_client.expire(key, REDIS_STREAK_TTL_SEC)
            else:
                redis_client.delete(key)
                streak = 0
        except RedisError as e:
            # если Redis недоступен — не валим сервис, но streak будет некорректен
            print(f"[Redis] ERROR while updating streak: {e}")
            streak = 0

        OVERHEAT_STREAK.labels(device_id=str(device_id)).set(streak)

        if streak >= OVERHEAT_CONSECUTIVE_REQUIRED:
            rule_id = "OVERHEAT_RISK_10"
            triggered.append(rule_id)

            rules_triggered_total.labels(rule_id=rule_id).inc()
            RULE_TRIGGERED_TOTAL.labels(
                rule_id=rule_id, severity="MEDIUM").inc()

            create_alert(
                rule_id=rule_id,
                severity="MEDIUM",
                message=f"Overheat risk: temperature>{OVERHEAT_THRESHOLD}C for {OVERHEAT_CONSECUTIVE_REQUIRED} consecutive messages",
                telemetry=telemetry
            )

            # сброс, чтобы не спамить
            try:
                redis_client.delete(key)
            except RedisError:
                pass

            OVERHEAT_STREAK.labels(device_id=str(device_id)).set(0)

    return triggered


# =========================
# Metrics server
# =========================
start_http_server(METRICS_PORT)
print(f"[Metrics] Prometheus metrics available on :{METRICS_PORT}/metrics")


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
    start = time.time()
    try:
        telemetry = json.loads(body.decode("utf-8"))

        telemetry_messages_processed_total.inc()

        print("\n--- New message received ---")
        print(json.dumps(telemetry, indent=2, ensure_ascii=False))

        triggered = apply_rules(telemetry)

        if triggered:
            print(f"!!! RULES TRIGGERED: {triggered}")
            MESSAGES_TOTAL.labels(status="ok").inc()
        else:
            print("No rules triggered.")

        time.sleep(2)

        PROCESSING_SECONDS.observe(time.time() - start)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        MESSAGES_TOTAL.labels(status="error").inc()
        PROCESSING_SECONDS.observe(time.time() - start)
        print(f"ERROR while processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


channel.basic_qos(prefetch_count=10)

channel.basic_consume(
    queue=QUEUE_NAME,
    on_message_callback=on_message,
    auto_ack=False
)

channel.start_consuming()
