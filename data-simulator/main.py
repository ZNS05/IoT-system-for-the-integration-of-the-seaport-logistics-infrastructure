import os
import asyncio
import random
from datetime import datetime, timezone

import httpx

API_URL = os.getenv("API_URL", "http://iot-controller:8000/ingest")
DEVICE_COUNT = int(os.getenv("DEVICE_COUNT", "50"))
MSG_RATE_PER_DEVICE = float(os.getenv("MSG_RATE_PER_DEVICE", "1"))

CRANE_RATIO = float(os.getenv("CRANE_RATIO", "0.2"))
FORKLIFT_RATIO = float(os.getenv("FORKLIFT_RATIO", "0.5"))
TRUCK_RATIO = float(os.getenv("TRUCK_RATIO", "0.3"))

OVERLOAD_PROB = float(os.getenv("OVERLOAD_PROB", "0.05"))

OVERHEAT_PROB = float(os.getenv("OVERHEAT_PROB", "0.10"))
OVERHEAT_EPISODE_LEN = int(os.getenv("OVERHEAT_EPISODE_LEN", "10"))

# гарант: хотя бы один перегруз каждые N сообщений для первого крана
FORCE_OVERLOAD_EVERY = int(os.getenv("FORCE_OVERLOAD_EVERY", "15"))

LOCATIONS = ["berth-1", "berth-2", "berth-3",
             "yard-A", "yard-B", "gate-1", "gate-2"]

overheat_left: dict[int, int] = {}
# чтобы принудительно делать перегруз периодически
msg_counter: dict[int, int] = {}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def pick_device_types(n: int) -> list[str]:
    total = CRANE_RATIO + FORKLIFT_RATIO + TRUCK_RATIO
    w = [CRANE_RATIO / total, FORKLIFT_RATIO / total, TRUCK_RATIO / total]
    types = random.choices(["crane", "forklift", "truck"], weights=w, k=n)

    # ГАРАНТИЯ: хотя бы один кран
    if "crane" not in types:
        types[0] = "crane"
    return types


def should_overheat(device_id: int) -> bool:
    left = overheat_left.get(device_id, 0)
    if left > 0:
        overheat_left[device_id] = left - 1
        return True

    if random.random() < OVERHEAT_PROB:
        overheat_left[device_id] = OVERHEAT_EPISODE_LEN - 1
        return True

    return False


def should_overload(device_id: int, device_type: str) -> bool:
    if device_type != "crane":
        return False

    # принудительный перегруз для device_id=1 раз в FORCE_OVERLOAD_EVERY сообщений
    c = msg_counter.get(device_id, 0) + 1
    msg_counter[device_id] = c
    if device_id == 1 and FORCE_OVERLOAD_EVERY > 0 and (c % FORCE_OVERLOAD_EVERY == 0):
        return True

    # обычная вероятность
    return random.random() < OVERLOAD_PROB


def generate_payload(device_id: int, device_type: str) -> dict:
    location = random.choice(LOCATIONS)
    status = random.choice(["operating", "idle"])

    # Температура: эпизоды по 10 подряд
    if should_overheat(device_id):
        temperature = random.uniform(61, 75)
    else:
        temperature = random.uniform(25, 55)

    # Нагрузка: ТОЛЬКО для crane и ВСЕГДА числом
    load_weight = None
    if device_type == "crane":
        if should_overload(device_id, device_type):
            load_weight = random.uniform(21, 35)
        else:
            load_weight = random.uniform(5, 18)

    return {
        "device_id": device_id,
        "device_type": device_type,
        "location": location,
        "load_weight": None if load_weight is None else float(round(load_weight, 2)),
        "status": status,
        "temperature": float(round(temperature, 2)),
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
    print(
        f"OVERLOAD_PROB={OVERLOAD_PROB}, FORCE_OVERLOAD_EVERY={FORCE_OVERLOAD_EVERY}")
    print(
        f"OVERHEAT_PROB={OVERHEAT_PROB}, OVERHEAT_EPISODE_LEN={OVERHEAT_EPISODE_LEN}")

    device_types = pick_device_types(DEVICE_COUNT)
    print("Device types:", device_types)

    async with httpx.AsyncClient() as client:
        tasks = []
        for i in range(DEVICE_COUNT):
            device_id = i + 1
            tasks.append(asyncio.create_task(
                device_loop(client, device_id, device_types[i])))
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
