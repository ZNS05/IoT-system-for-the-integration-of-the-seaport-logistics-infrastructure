from contextlib import asynccontextmanager
from datetime import datetime, timezone
from enum import Enum
import json
import os
import queue
import threading
import time
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from prometheus_fastapi_instrumentator import Instrumentator
from pymongo import MongoClient
from pymongo.errors import CollectionInvalid, PyMongoError
import pika
from pika.exceptions import AMQPConnectionError, StreamLostError


SERVICE_NAME = "iot-controller"

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "iot_port")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "measurements")

RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
RABBIT_EXCHANGE = os.getenv("RABBIT_EXCHANGE", "iot_data")
RABBIT_QUEUE = os.getenv("RABBIT_QUEUE", "rule_engine_input")
PUBLISH_QUEUE_MAX = int(os.getenv("PUBLISH_QUEUE_MAX", "5000"))
RABBIT_RECONNECT_DELAY_SEC = float(os.getenv("RABBIT_RECONNECT_DELAY_SEC", "2"))

mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
db = mongo_client[MONGO_DB]
measurements_col = db[MONGO_COLLECTION]

publish_queue: "queue.Queue[dict]" = queue.Queue(maxsize=PUBLISH_QUEUE_MAX)
stop_event = threading.Event()
publisher_ready = threading.Event()


class DeviceType(str, Enum):
    crane = "crane"
    forklift = "forklift"
    truck = "truck"


class DeviceStatus(str, Enum):
    operating = "operating"
    idle = "idle"
    error = "error"


class Telemetry(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    device_id: int = Field(..., ge=1, description="Unique device ID")
    device_type: DeviceType = Field(..., description="crane/forklift/truck")
    location: str = Field(..., min_length=1, description="Terminal location")
    load_weight: Optional[float] = Field(
        None, ge=0, description="Current load weight in tons"
    )
    status: Optional[DeviceStatus] = Field(None, description="operating/idle/error")
    temperature: Optional[float] = Field(None, description="Equipment temperature, C")
    timestamp: Optional[datetime] = None


def log_json(service: str, level: str, event: str, **fields):
    payload = {
        "ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "service": service,
        "level": level,
        "event": event,
        **fields,
    }
    print(json.dumps(payload, ensure_ascii=False), flush=True)


def ensure_mongo_collection():
    try:
        if MONGO_COLLECTION not in db.list_collection_names():
            try:
                db.create_collection(
                    MONGO_COLLECTION,
                    timeseries={
                        "timeField": "timestamp",
                        "metaField": "device_id",
                        "granularity": "seconds",
                    },
                )
                log_json(SERVICE_NAME, "INFO", "MONGO_TIMESERIES_CREATED")
            except CollectionInvalid:
                pass

        measurements_col.create_index([("device_id", 1), ("timestamp", -1)])
        measurements_col.create_index([("device_type", 1), ("timestamp", -1)])
    except PyMongoError as exc:
        log_json(SERVICE_NAME, "ERROR", "MONGO_INIT_FAILED", error=str(exc))
        raise


def rabbit_connect():
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        heartbeat=60,
        blocked_connection_timeout=30,
        connection_attempts=3,
        retry_delay=2,
    )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(
        exchange=RABBIT_EXCHANGE,
        exchange_type="fanout",
        durable=True,
    )
    channel.queue_declare(queue=RABBIT_QUEUE, durable=True)
    channel.queue_bind(exchange=RABBIT_EXCHANGE, queue=RABBIT_QUEUE)
    channel.confirm_delivery()
    return connection, channel


def rabbit_publish(channel, doc: dict):
    body = json.dumps(doc, default=str).encode("utf-8")
    channel.basic_publish(
        exchange=RABBIT_EXCHANGE,
        routing_key="",
        body=body,
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,
        ),
    )


def close_rabbit(connection):
    try:
        if connection is not None and connection.is_open:
            connection.close()
    except Exception:
        pass


def rabbit_publisher_worker():
    connection = None
    channel = None

    while not stop_event.is_set():
        try:
            if connection is None or connection.is_closed or channel is None or channel.is_closed:
                connection, channel = rabbit_connect()
                publisher_ready.set()
                log_json(SERVICE_NAME, "INFO", "RABBIT_CONNECTED")
        except (AMQPConnectionError, OSError, StreamLostError) as exc:
            publisher_ready.clear()
            close_rabbit(connection)
            connection = None
            channel = None
            log_json(SERVICE_NAME, "WARN", "RABBIT_CONNECT_RETRY", error=str(exc))
            time.sleep(RABBIT_RECONNECT_DELAY_SEC)
            continue

        try:
            doc = publish_queue.get(timeout=1)
        except queue.Empty:
            try:
                connection.process_data_events(time_limit=0)
            except Exception as exc:
                publisher_ready.clear()
                close_rabbit(connection)
                connection = None
                channel = None
                log_json(
                    SERVICE_NAME,
                    "WARN",
                    "RABBIT_HEARTBEAT_FAILED",
                    error=str(exc),
                )
            continue

        published = False
        while not published and not stop_event.is_set():
            try:
                if connection is None or connection.is_closed or channel is None or channel.is_closed:
                    connection, channel = rabbit_connect()
                    publisher_ready.set()

                rabbit_publish(channel, doc)
                published = True
                log_json(
                    SERVICE_NAME,
                    "INFO",
                    "RABBIT_PUBLISHED",
                    device_id=doc.get("device_id"),
                    queue_size=publish_queue.qsize(),
                )
            except Exception as exc:
                publisher_ready.clear()
                close_rabbit(connection)
                connection = None
                channel = None
                log_json(
                    SERVICE_NAME,
                    "ERROR",
                    "RABBIT_PUBLISH_RETRY",
                    device_id=doc.get("device_id"),
                    error=str(exc),
                )
                time.sleep(RABBIT_RECONNECT_DELAY_SEC)

        publish_queue.task_done()

    publisher_ready.clear()
    close_rabbit(connection)


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_mongo_collection()
    stop_event.clear()
    publisher_thread = threading.Thread(target=rabbit_publisher_worker, daemon=True)
    publisher_thread.start()
    yield
    stop_event.set()
    publisher_thread.join(timeout=5)


app = FastAPI(title="IoT Controller - Port Terminal", lifespan=lifespan)
Instrumentator().instrument(app).expose(app, endpoint="/metrics")


@app.get("/health")
def health():
    checks = {
        "mongo": False,
        "rabbit_publisher": publisher_ready.is_set(),
        "publish_queue_size": publish_queue.qsize(),
    }

    try:
        mongo_client.admin.command("ping")
        checks["mongo"] = True
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={**checks, "error": f"MongoDB unavailable: {exc}"},
        )

    if not checks["rabbit_publisher"]:
        raise HTTPException(status_code=503, detail=checks)

    return {"status": "ok", **checks}


@app.post("/ingest")
def ingest(payload: Telemetry):
    doc = payload.model_dump()
    if doc["timestamp"] is None:
        doc["timestamp"] = datetime.now(timezone.utc)

    log_json(
        SERVICE_NAME,
        "INFO",
        "INGEST_RECEIVED",
        device_id=doc.get("device_id"),
        device_type=doc.get("device_type"),
        location=doc.get("location"),
    )

    try:
        result = measurements_col.insert_one(doc)
    except Exception as exc:
        log_json(SERVICE_NAME, "ERROR", "MONGO_INSERT_FAILED", error=str(exc))
        raise HTTPException(status_code=500, detail=f"MongoDB error: {exc}")

    try:
        publish_queue.put_nowait(doc)
    except queue.Full:
        log_json(
            SERVICE_NAME,
            "WARN",
            "PUBLISH_QUEUE_FULL",
            device_id=doc.get("device_id"),
            queue_size=publish_queue.qsize(),
        )
        raise HTTPException(status_code=503, detail="Publish queue is full, try later")

    return {
        "inserted_id": str(result.inserted_id),
        "queued_for_processing": True,
        "publish_queue_size": publish_queue.qsize(),
    }
