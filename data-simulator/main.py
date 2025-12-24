import os
import asyncio
import random
from datetime import datetime, timezone

import httpx


API_URL = os.getenv("API_URL", "http://iot-controller:8000/ingest")
DEVICE_COUNT = int(os.getenv("DEVICE_COUNT", "50"))

# сообщений в секунду на устройство (например 1 = раз в 1 сек)
MSG_RATE_PER_DEVICE = float(os.getenv("MSG_RATE_PER_DEVICE", "1"))

# доли типов техники (сумма не обязана быть 1 — нормализуем)
CRANE_RATIO = float(os.getenv("CRANE_RATIO", "0.2"))
FORKLIFT_RATIO = float(os.getenv("FORKLIFT_RATIO", "0.5"))
TRUCK_RATIO = float(os.getenv("TRUCK_RATIO", "0.3"))

# вероятность “спайка”, чтобы правила иногда срабатывали
OVERLOAD_PROB = float(os.getenv("OVERLOAD_PROB", "0.05"))   # перегруз крана
OVERHEAT_PROB = float(os.getenv("OVERHEAT_PROB", "0.10"))   # перегрев техники

LOCATIONS = ["berth-1", "berth-2", "berth-3",
             "yard-A", "yard-B", "gate-1", "gate-2"]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def pick_device_types(n: int) -> list[str]:
    total = CRANE_RATIO + FORKLIFT_RATIO + TRUCK_RATIO
    w = [CRANE_RATIO / total, FORKLIFT_RATIO / total, TRUCK_RATIO / total]
    return random.choices(["crane", "forklift", "truck"], weights=w, k=n)


def generate_payload(device_id: int, device_type: str) -> dict:
    location = random.choice(LOCATIONS)

    # базовые значения
    temperature = random.uniform(25, 55)
    load_weight = None

    status = random.choice(["operating", "idle"])

    if device_type == "crane":
        # обычный вес 5..18 тонн, иногда перегруз > 20
        if random.random() < OVERLOAD_PROB:
            # чтобы CRANE_OVERLOAD точно сработал
            load_weight = random.uniform(21, 35)
        else:
            load_weight = random.uniform(5, 18)

    # перегрев иногда для любого типа (в т.ч. forklift/truck)
    if random.random() < OVERHEAT_PROB:
        temperature = random.uniform(61, 75)

    return {
        "device_id": device_id,
        "device_type": device_type,
        "location": location,
        "load_weight": load_weight,
        "status": status,
        "temperature": round(temperature, 2),
        "timestamp": utc_now_iso(),
    }


async def device_loop(client: httpx.AsyncClient, device_id: int, device_type: str):
    period = 1.0 / MSG_RATE_PER_DEVICE if MSG_RATE_PER_DEVICE > 0 else 1.0

    while True:
        payload = generate_payload(device_id, device_type)
        try:
            r = await client.post(API_URL, json=payload, timeout=10)
            if r.status_code >= 400:
                print(f"[device {device_id}] HTTP {r.status_code}: {r.text}")
        except Exception as e:
            print(f"[device {device_id}] error: {e}")

        await asyncio.sleep(period)


async def main():
    print("Data Simulator starting...")
    print(f"API_URL={API_URL}")
    print(
        f"DEVICE_COUNT={DEVICE_COUNT}, MSG_RATE_PER_DEVICE={MSG_RATE_PER_DEVICE}")

    device_types = pick_device_types(DEVICE_COUNT)

    async with httpx.AsyncClient() as client:
        tasks = []
        for i in range(DEVICE_COUNT):
            device_id = i + 1
            tasks.append(asyncio.create_task(
                device_loop(client, device_id, device_types[i])))

        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
