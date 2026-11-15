"""Ghost-kitchen orders: a week of history for the bucket, or a live stream for Kafka.
 
    python generate_orders.py files    # 7 days of events -> s3://lakehouse/landing/
    python generate_orders.py stream   # one day of events, live -> Kafka topic `orders`
"""
 
import io
import json
import random
import sys
import time
from datetime import datetime, timedelta, timezone
 
import pandas as pd
from common import KAFKA_BOOTSTRAP, s3
 
LOCATIONS = {1: "Oakland", 2: "Berkeley", 3: "San Francisco", 4: "San Jose"}
BRANDS = {"Pizza Planet": 16.0, "Wok This Way": 21.0, "Taco Loco": 12.5,
          "Curry House": 23.0, "Sushi Express": 29.0}
rng = random.Random(7)
 
 
def order(ts):
    """One order's events: created, ready in the kitchen, delivered."""
    order_id = f"{rng.getrandbits(40):010x}"
    location = rng.choice(list(LOCATIONS))
    brand, items = rng.choice(list(BRANDS)), rng.randint(1, 4)
    total = round(BRANDS[brand] * items * rng.uniform(0.9, 1.2), 2)
    steps = [("order_created", 0), ("order_ready", rng.randint(8, 20)),
             ("delivered", rng.randint(25, 50))]
    return [{
        "event_id": f"{order_id}-{n}",
        "event_type": kind,
        "ts": (ts + timedelta(minutes=minutes)).isoformat(timespec="seconds"),
        "location_id": location,
        "order_id": order_id,
        "body": json.dumps({"brand": brand, "items": items, "total": total}),
    } for n, (kind, minutes) in enumerate(steps)]
 
 
def messy(events):
    """Real feeds misbehave: a few events lose their location, some arrive twice."""
    out = []
    for event in events:
        if rng.random() < 0.01:
            event["location_id"] = None
        out.append(event)
        if rng.random() < 0.03:
            out.append(dict(event))
    return out
 
 
def week():
    start = datetime(2026, 9, 1)
    for day in range(7):
        events = []
        for _ in range(rng.randint(1200, 1800)):
            events += order(start + timedelta(days=day, seconds=rng.randint(36000, 79200)))
        yield (start + timedelta(days=day)).date(), messy(events)
 
 
def put_parquet(client, key, frame):
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    client.put_object(Bucket="lakehouse", Key=key, Body=buffer.getvalue())
    print(f"{key}  {len(frame):,} rows")
 
 
def write_files():
    client = s3()
    for day, events in week():
        frame = pd.DataFrame(events).astype({"location_id": "Int64"})  # keep the gaps as nulls
        put_parquet(client, f"landing/orders/{day}.parquet", frame)
    cities = pd.DataFrame({"location_id": list(LOCATIONS), "city": list(LOCATIONS.values())})
    put_parquet(client, "landing/locations/locations.parquet", cities)
 
 
def stream(per_second=50):
    from kafka import KafkaProducer
 
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP, key_serializer=str.encode,
                             value_serializer=lambda v: json.dumps(v).encode())
    rng.seed()  # new order ids on every run
    _, events = next(week())
    events.sort(key=lambda e: e["ts"])
    stamps = {}
    for n, event in enumerate(events, 1):
        if event["event_id"] not in stamps:  # a resent event keeps its first timestamp
            late = rng.random() < 0.02  # and a few arrive three minutes late
            now = datetime.now(timezone.utc) - timedelta(minutes=3 if late else 0)
            stamps[event["event_id"]] = now.isoformat(timespec="milliseconds")
        producer.send("orders", key=event["order_id"],
                      value={**event, "ts": stamps[event["event_id"]]})
        if n % 1000 == 0:
            print(f"sent {n:,} events")
        time.sleep(1 / per_second)
    producer.flush()
    print(f"sent {len(events):,} events to the topic orders")
 
 
if __name__ == "__main__":
    {"files": write_files, "stream": stream}[sys.argv[1]]()