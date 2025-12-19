from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional
from pymongo import MongoClient
import json
import pika
from pika.exceptions import AMQPConnectionError, StreamLostError
import os

# MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["iot_port"]
measurements_col = db["measurements"]


# RabbitMQ
RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
RABBIT_EXCHANGE = os.getenv("RABBIT_EXCHANGE", "iot_data")

rabbit_connection = None
rabbit_channel = None


def rabbit_connect():
    """
    Создаёт новое соединение и канал + гарантирует наличие exchange.
    """
    global rabbit_connection, rabbit_channel

    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        heartbeat=60,                 # поддерживаем соединение живым
        blocked_connection_timeout=30  # чтобы не зависать бесконечно
    )

    rabbit_connection = pika.BlockingConnection(params)
    rabbit_channel = rabbit_connection.channel()
    rabbit_channel.exchange_declare(
        exchange=RABBIT_EXCHANGE,
        exchange_type="fanout",
        durable=True
    )


def rabbit_publish(doc: dict):
    """
    Публикует сообщение. Если соединение потеряно — переподключается и повторяет 1 раз.
    """
    global rabbit_connection, rabbit_channel

    if rabbit_connection is None or rabbit_connection.is_closed:
        rabbit_connect()
    if rabbit_channel is None or rabbit_channel.is_closed:
        rabbit_channel = rabbit_connection.channel()

    body = json.dumps(doc, default=str).encode("utf-8")

    try:
        rabbit_channel.basic_publish(
            exchange=RABBIT_EXCHANGE,
            routing_key="",
            body=body,
        )
    except (StreamLostError, AMQPConnectionError, ConnectionError, OSError):
        # Переподключение и повтор одной попытки
        rabbit_connect()
        rabbit_channel.basic_publish(
            exchange=RABBIT_EXCHANGE,
            routing_key="",
            body=body,
        )


# FastAPI
app = FastAPI(title="IoT Controller - Port Terminal")


@app.on_event("startup")
def on_startup():
    rabbit_connect()


@app.on_event("shutdown")
def on_shutdown():
    global rabbit_connection
    try:
        if rabbit_connection is not None and rabbit_connection.is_open:
            rabbit_connection.close()
    except Exception:
        pass


# Models
class Telemetry(BaseModel):
    device_id: int = Field(..., ge=1, description="Уникальный ID устройства")
    device_type: str = Field(...,
                             description="Тип устройства: crane/forklift/truck")
    location: str = Field(...,
                          description="Локация на терминале, например berth-3")
    load_weight: Optional[float] = Field(
        None, description="Вес текущего груза в тоннах")
    status: Optional[str] = Field(None, description="operating/idle/error")
    temperature: Optional[float] = Field(
        None, description="Температура узла, °C")
    # если не придёт, поставим текущий момент
    timestamp: Optional[datetime] = None


# Endpoints
@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/ingest")
def ingest(payload: Telemetry):
    doc = payload.dict()
    if doc["timestamp"] is None:
        doc["timestamp"] = datetime.utcnow()

    # MongoDB
    try:
        result = measurements_col.insert_one(doc)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MongoDB error: {e}")

    # RabbitMQ
    try:
        rabbit_publish(doc)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"RabbitMQ error: {e}")

    return {"inserted_id": str(result.inserted_id)}
