from datetime import datetime, timezone
import json
import os
import threading
import time

import pika
from pika.exceptions import AMQPConnectionError, StreamLostError
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from pymongo import MongoClient
import redis
from redis.exceptions import RedisError


SERVICE_NAME = "rule-engine"

RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")
RABBIT_EXCHANGE = os.getenv("RABBIT_EXCHANGE", "iot_data")
QUEUE_NAME = os.getenv("RABBIT_QUEUE", "rule_engine_input")
RABBIT_PREFETCH_COUNT = int(os.getenv("RABBIT_PREFETCH_COUNT", "50"))

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "iot_port")
ALERTS_COLLECTION = os.getenv("ALERTS_COLLECTION", "alerts")

METRICS_PORT = int(os.getenv("METRICS_PORT", "9101"))
PROCESSING_DELAY_SEC = float(os.getenv("PROCESSING_DELAY_SEC", "0"))

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_STATE_TTL_SEC = int(os.getenv("REDIS_STATE_TTL_SEC", "3600"))

CRANE_OVERLOAD_THRESHOLD = float(os.getenv("CRANE_OVERLOAD_THRESHOLD", "20"))
OVERHEAT_THRESHOLD = float(os.getenv("OVERHEAT_THRESHOLD", "60"))
OVERHEAT_CONSECUTIVE_REQUIRED = int(os.getenv("OVERHEAT_CONSECUTIVE_REQUIRED", "10"))
ERROR_STATUS_CONSECUTIVE_REQUIRED = int(
    os.getenv("ERROR_STATUS_CONSECUTIVE_REQUIRED", "5")
)
CONNECTION_LOST_AFTER_SEC = int(os.getenv("CONNECTION_LOST_AFTER_SEC", "30"))
CONNECTION_LOSS_SCAN_INTERVAL_SEC = int(
    os.getenv("CONNECTION_LOSS_SCAN_INTERVAL_SEC", "5")
)
ANOMALY_TEMP_MIN = float(os.getenv("ANOMALY_TEMP_MIN", "-20"))
ANOMALY_TEMP_MAX = float(os.getenv("ANOMALY_TEMP_MAX", "100"))
ANOMALY_LOAD_MAX = float(os.getenv("ANOMALY_LOAD_MAX", "50"))


mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
db = mongo_client[MONGO_DB]
alerts_col = db[ALERTS_COLLECTION]

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


MESSAGES_TOTAL = Counter(
    "rule_engine_messages_total",
    "Total messages processed by rule-engine",
    ["status"],
)
RULE_TRIGGERED_TOTAL = Counter(
    "rule_engine_rule_triggered_total",
    "How many times a rule was triggered",
    ["rule_id", "severity"],
)
PROCESSING_SECONDS = Histogram(
    "rule_engine_processing_seconds",
    "Time spent processing one message",
)
OVERHEAT_STREAK = Gauge(
    "rule_engine_overheat_streak",
    "Current overheat streak per device_id",
    ["device_id"],
)
ERROR_STATUS_STREAK = Gauge(
    "rule_engine_error_status_streak",
    "Current error-status streak per device_id",
    ["device_id"],
)

telemetry_messages_processed_total = Counter(
    "telemetry_messages_processed_total",
    "Total number of telemetry messages processed by rule-engine",
)
alerts_created_total = Counter(
    "alerts_created_total",
    "Total number of alerts created by rule-engine",
)
rules_triggered_total = Counter(
    "rules_triggered_total",
    "Total number of triggered rules by rule-engine",
    ["rule_id"],
)
telemetry_processing_seconds = Histogram(
    "telemetry_processing_seconds",
    "Time spent processing a telemetry message in rule-engine",
)

monitor_stop = threading.Event()


def log_json(service: str, level: str, event: str, **fields):
    payload = {
        "ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "service": service,
        "level": level,
        "event": event,
        **fields,
    }
    print(json.dumps(payload, ensure_ascii=False), flush=True)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def to_float(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    try:
        return float(value)
    except Exception:
        return None


def to_int(value):
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


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

    res = alerts_col.insert_one(doc)
    alerts_created_total.inc()
    rules_triggered_total.labels(rule_id=rule_id).inc()
    RULE_TRIGGERED_TOTAL.labels(rule_id=rule_id, severity=severity).inc()
    log_json(
        SERVICE_NAME,
        "WARN",
        "ALERT_CREATED",
        rule_id=rule_id,
        severity=severity,
        alert_id=str(res.inserted_id),
        device_id=telemetry.get("device_id"),
    )


def redis_key(prefix: str, device_id: int) -> str:
    return f"{prefix}:{device_id}"


def redis_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def update_device_last_seen(telemetry: dict):
    device_id = to_int(telemetry.get("device_id"))
    if device_id is None:
        return

    try:
        redis_client.sadd("known_devices", str(device_id))
        redis_client.hset(
            redis_key("device_state", device_id),
            mapping={
                "last_seen": str(time.time()),
                "device_type": telemetry.get("device_type") or "",
                "location": telemetry.get("location") or "",
            },
        )
        redis_client.expire(redis_key("device_state", device_id), REDIS_STATE_TTL_SEC)
        redis_client.delete(redis_key("connection_lost_alerted", device_id))
    except RedisError as exc:
        log_json(SERVICE_NAME, "ERROR", "REDIS_LAST_SEEN_FAILED", error=str(exc))


def track_streak(device_id: int, prefix: str, active: bool) -> int:
    streak_key = redis_key(prefix, device_id)
    alerted_key = redis_key(f"{prefix}_alerted", device_id)

    if active:
        streak = redis_int(redis_client.incr(streak_key))
        redis_client.expire(streak_key, REDIS_STATE_TTL_SEC)
        return streak

    redis_client.delete(streak_key)
    redis_client.delete(alerted_key)
    return 0


def alert_once_per_episode(device_id: int, prefix: str) -> bool:
    alerted_key = redis_key(f"{prefix}_alerted", device_id)
    return bool(redis_client.set(alerted_key, "1", nx=True, ex=REDIS_STATE_TTL_SEC))


def apply_crane_overload_rule(telemetry: dict, triggered: list[str]):
    device_type = telemetry.get("device_type")
    load_weight = to_float(telemetry.get("load_weight"))

    if (
        device_type == "crane"
        and load_weight is not None
        and load_weight > CRANE_OVERLOAD_THRESHOLD
    ):
        rule_id = "CRANE_OVERLOAD"
        triggered.append(rule_id)
        create_alert(
            rule_id=rule_id,
            severity="HIGH",
            message=(
                f"Crane overload: load_weight={load_weight}t "
                f"> {CRANE_OVERLOAD_THRESHOLD}t"
            ),
            telemetry=telemetry,
        )


def apply_overheat_rule(telemetry: dict, triggered: list[str]):
    device_id = to_int(telemetry.get("device_id"))
    if device_id is None:
        return

    temperature = to_float(telemetry.get("temperature"))

    try:
        streak = track_streak(
            device_id,
            "overheat_streak",
            temperature is not None and temperature > OVERHEAT_THRESHOLD,
        )
        OVERHEAT_STREAK.labels(device_id=str(device_id)).set(streak)

        if streak >= OVERHEAT_CONSECUTIVE_REQUIRED and alert_once_per_episode(
            device_id, "overheat_streak"
        ):
            rule_id = "OVERHEAT_RISK_10"
            triggered.append(rule_id)
            create_alert(
                rule_id=rule_id,
                severity="MEDIUM",
                message=(
                    f"Overheat risk: temperature>{OVERHEAT_THRESHOLD}C for "
                    f"{OVERHEAT_CONSECUTIVE_REQUIRED} consecutive messages"
                ),
                telemetry=telemetry,
            )
    except RedisError as exc:
        log_json(SERVICE_NAME, "ERROR", "REDIS_OVERHEAT_FAILED", error=str(exc))


def apply_error_status_rule(telemetry: dict, triggered: list[str]):
    device_id = to_int(telemetry.get("device_id"))
    if device_id is None:
        return

    try:
        streak = track_streak(
            device_id,
            "error_status_streak",
            telemetry.get("status") == "error",
        )
        ERROR_STATUS_STREAK.labels(device_id=str(device_id)).set(streak)

        if streak >= ERROR_STATUS_CONSECUTIVE_REQUIRED and alert_once_per_episode(
            device_id, "error_status_streak"
        ):
            rule_id = "ERROR_STATUS_DURATION"
            triggered.append(rule_id)
            create_alert(
                rule_id=rule_id,
                severity="HIGH",
                message=(
                    "Device remains in error status for "
                    f"{ERROR_STATUS_CONSECUTIVE_REQUIRED} consecutive messages"
                ),
                telemetry=telemetry,
            )
    except RedisError as exc:
        log_json(SERVICE_NAME, "ERROR", "REDIS_ERROR_STATUS_FAILED", error=str(exc))


def apply_anomaly_rule(telemetry: dict, triggered: list[str]):
    anomalies = []
    temperature = to_float(telemetry.get("temperature"))
    load_weight = to_float(telemetry.get("load_weight"))

    if temperature is not None and (
        temperature < ANOMALY_TEMP_MIN or temperature > ANOMALY_TEMP_MAX
    ):
        anomalies.append(f"temperature={temperature}")

    if load_weight is not None and (load_weight < 0 or load_weight > ANOMALY_LOAD_MAX):
        anomalies.append(f"load_weight={load_weight}")

    if anomalies:
        rule_id = "ANOMALOUS_TELEMETRY"
        triggered.append(rule_id)
        create_alert(
            rule_id=rule_id,
            severity="MEDIUM",
            message="Anomalous telemetry values: " + ", ".join(anomalies),
            telemetry=telemetry,
        )


def apply_rules(telemetry: dict):
    triggered: list[str] = []
    update_device_last_seen(telemetry)
    apply_crane_overload_rule(telemetry, triggered)
    apply_overheat_rule(telemetry, triggered)
    apply_error_status_rule(telemetry, triggered)
    apply_anomaly_rule(telemetry, triggered)
    return triggered


def connection_loss_monitor():
    while not monitor_stop.is_set():
        try:
            device_ids = redis_client.smembers("known_devices")
            now = time.time()

            for raw_device_id in device_ids:
                device_id = to_int(raw_device_id)
                if device_id is None:
                    continue

                state_key = redis_key("device_state", device_id)
                state = redis_client.hgetall(state_key)
                last_seen = to_float(state.get("last_seen"))
                if last_seen is None:
                    continue

                if now - last_seen <= CONNECTION_LOST_AFTER_SEC:
                    continue

                alert_key = redis_key("connection_lost_alerted", device_id)
                if not redis_client.set(
                    alert_key,
                    "1",
                    nx=True,
                    ex=max(CONNECTION_LOST_AFTER_SEC * 2, REDIS_STATE_TTL_SEC),
                ):
                    continue

                telemetry = {
                    "device_id": device_id,
                    "device_type": state.get("device_type") or None,
                    "location": state.get("location") or None,
                    "timestamp": datetime.fromtimestamp(
                        last_seen, tz=timezone.utc
                    ).isoformat(),
                    "last_seen_age_sec": round(now - last_seen, 2),
                }
                create_alert(
                    rule_id="DEVICE_CONNECTION_LOST",
                    severity="HIGH",
                    message=(
                        "No telemetry from device for "
                        f"{round(now - last_seen, 2)} seconds"
                    ),
                    telemetry=telemetry,
                )
        except RedisError as exc:
            log_json(SERVICE_NAME, "ERROR", "CONNECTION_MONITOR_REDIS_FAILED", error=str(exc))
        except Exception as exc:
            log_json(SERVICE_NAME, "ERROR", "CONNECTION_MONITOR_FAILED", error=str(exc))

        monitor_stop.wait(CONNECTION_LOSS_SCAN_INTERVAL_SEC)


def connect_rabbit_with_retry():
    delay = 2
    while True:
        try:
            log_json(SERVICE_NAME, "INFO", "RABBIT_CONNECTING", host=RABBIT_HOST)
            conn = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBIT_HOST, heartbeat=60)
            )
            log_json(SERVICE_NAME, "INFO", "RABBIT_CONNECTED")
            return conn
        except AMQPConnectionError as exc:
            log_json(
                SERVICE_NAME,
                "WARN",
                "RABBIT_CONNECT_RETRY",
                error=str(exc),
                retry_in_sec=delay,
            )
            time.sleep(delay)


def configure_channel(connection):
    channel = connection.channel()
    channel.exchange_declare(
        exchange=RABBIT_EXCHANGE,
        exchange_type="fanout",
        durable=True,
    )
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_bind(exchange=RABBIT_EXCHANGE, queue=QUEUE_NAME)
    channel.basic_qos(prefetch_count=RABBIT_PREFETCH_COUNT)
    return channel


def on_message(ch, method, properties, body):
    start = time.time()
    try:
        telemetry = json.loads(body.decode("utf-8"))
        telemetry_messages_processed_total.inc()

        triggered = apply_rules(telemetry)
        if triggered:
            log_json(
                SERVICE_NAME,
                "WARN",
                "RULES_TRIGGERED",
                rules=triggered,
                device_id=telemetry.get("device_id"),
            )
        else:
            log_json(
                SERVICE_NAME,
                "INFO",
                "MESSAGE_PROCESSED",
                device_id=telemetry.get("device_id"),
            )

        if PROCESSING_DELAY_SEC > 0:
            time.sleep(PROCESSING_DELAY_SEC)

        elapsed = time.time() - start
        PROCESSING_SECONDS.observe(elapsed)
        telemetry_processing_seconds.observe(elapsed)
        MESSAGES_TOTAL.labels(status="ok").inc()
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except json.JSONDecodeError as exc:
        elapsed = time.time() - start
        PROCESSING_SECONDS.observe(elapsed)
        telemetry_processing_seconds.observe(elapsed)
        MESSAGES_TOTAL.labels(status="error").inc()
        log_json(SERVICE_NAME, "ERROR", "MESSAGE_JSON_INVALID", error=str(exc))
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as exc:
        elapsed = time.time() - start
        PROCESSING_SECONDS.observe(elapsed)
        telemetry_processing_seconds.observe(elapsed)
        MESSAGES_TOTAL.labels(status="error").inc()
        log_json(SERVICE_NAME, "ERROR", "MESSAGE_PROCESSING_FAILED", error=str(exc))
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def consume_forever():
    while True:
        connection = None
        try:
            connection = connect_rabbit_with_retry()
            channel = configure_channel(connection)
            channel.basic_consume(
                queue=QUEUE_NAME,
                on_message_callback=on_message,
                auto_ack=False,
            )
            log_json(
                SERVICE_NAME,
                "INFO",
                "RULE_ENGINE_STARTED",
                queue=QUEUE_NAME,
                prefetch=RABBIT_PREFETCH_COUNT,
            )
            channel.start_consuming()
        except KeyboardInterrupt:
            monitor_stop.set()
            break
        except (AMQPConnectionError, StreamLostError, OSError) as exc:
            log_json(SERVICE_NAME, "ERROR", "RABBIT_CONSUMER_RESTART", error=str(exc))
            time.sleep(2)
        except Exception as exc:
            log_json(SERVICE_NAME, "ERROR", "CONSUMER_RESTART", error=str(exc))
            time.sleep(2)
        finally:
            try:
                if connection is not None and connection.is_open:
                    connection.close()
            except Exception:
                pass


def main():
    mongo_client.admin.command("ping")
    redis_client.ping()
    alerts_col.create_index([("device_id", 1), ("created_at", -1)])
    alerts_col.create_index([("rule_id", 1), ("created_at", -1)])

    start_http_server(METRICS_PORT)
    log_json(SERVICE_NAME, "INFO", "METRICS_STARTED", port=METRICS_PORT)

    monitor_thread = threading.Thread(target=connection_loss_monitor, daemon=True)
    monitor_thread.start()

    consume_forever()


if __name__ == "__main__":
    main()
