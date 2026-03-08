import asyncio
import aiohttp
import aiohttp.web
import base64
import csv
import json
import struct
import zipfile
import io
import os
from datetime import datetime, timedelta

# ============================
# KONFIGURACJA OGÓLNA
# ============================
POLL_INTERVAL = 20  # sekundy
GTFS_CACHE_DIR = "gtfs_cache"
os.makedirs(GTFS_CACHE_DIR, exist_ok=True)

MIASTA = [
    {
        "nazwa": "augustow",
        "url_miasto": "augustow",
        "gtfs_source": "https://files.girlc.at/gtfs/augustow.zip",
        "realtime_format": "pb",  # json | pb | both
    },
    {
        "nazwa": "pksnova",
        "url_miasto": "pksnova",
        "gtfs_source": "https://files.girlc.at/gtfs/pks_nova.zip",
        "realtime_format": "pb",  # json | pb | both
    },
]

# ============================
# POMOCNICZE
# ============================
def encode_trip_execution(trip_execution_id: str) -> str:
    return base64.b64encode(trip_execution_id.encode()).decode()

def parse_time_string(time_str: str) -> datetime:
    """
    Obsługiwane formaty:
    - "6 min"
    - "< 1 min"
    - "<5 min"
    - "14:11"
    - "teraz"
    - inne / puste → teraz
    """
    now = datetime.now()
    if not time_str:
        return now

    time_str = time_str.strip().lower()

    # "< 1 min", "<5 min", "5 min"
    if "min" in time_str:
        digits = "".join(c for c in time_str if c.isdigit())
        if digits:
            return now + timedelta(minutes=int(digits))
        return now  # np. "< min"

    # "teraz"
    if time_str in ("teraz", "now", "—", "-"):
        return now

    # "HH:MM"
    try:
        return datetime.strptime(time_str, "%H:%M").replace(
            year=now.year,
            month=now.month,
            day=now.day
        )
    except ValueError:
        return now

# ============================
# GTFS-RT PROTOBUF ENCODER
# ============================
# Ręczna implementacja kodera protobuf dla GTFS-Realtime v2.0.
# Nie wymaga gtfs-realtime-bindings.
#
# Typy wire (protobuf):
#   0 = varint, 1 = 64-bit, 2 = length-delimited, 5 = 32-bit (float)
#
# Schemat GTFS-RT (uproszczony, pola użyte w tym skrypcie):
#
#   FeedMessage
#     1: FeedHeader header
#       1: string gtfs_realtime_version
#       2: uint64 timestamp
#     2: FeedEntity entity  [repeated]
#       1: string id
#       4: VehiclePosition vehicle
#         1: TripDescriptor trip
#           1: string trip_id
#         2: Position position
#           1: float latitude
#           2: float longitude
#           3: float bearing   (opcjonalne)
#           4: float speed     (opcjonalne)
#         3: VehicleDescriptor vehicle
#           1: string id
#           2: string label
#         5: VehicleStopStatus current_status (enum: 0=INCOMING_AT, 1=STOPPED_AT, 2=IN_TRANSIT_TO)

def _varint(value: int) -> bytes:
    """Koduje liczbę całkowitą jako protobuf varint."""
    result = []
    while True:
        bits = value & 0x7F
        value >>= 7
        if value:
            result.append(bits | 0x80)
        else:
            result.append(bits)
            break
    return bytes(result)

def _key(field: int, wire: int) -> bytes:
    return _varint((field << 3) | wire)

def _str(field: int, value: str) -> bytes:
    enc = value.encode("utf-8")
    return _key(field, 2) + _varint(len(enc)) + enc

def _uint64(field: int, value: int) -> bytes:
    return _key(field, 0) + _varint(value)

def _float32(field: int, value: float) -> bytes:
    """Koduje float jako 32-bitowy little-endian (wire type 5)."""
    return _key(field, 5) + struct.pack("<f", value)

def _enum(field: int, value: int) -> bytes:
    return _key(field, 0) + _varint(value)

def _msg(field: int, data: bytes) -> bytes:
    """Zagnieżdżona wiadomość (wire type 2)."""
    return _key(field, 2) + _varint(len(data)) + data


def _build_position(vehicle: dict) -> bytes:
    """Buduje Position z dict pojazdu."""
    pos = b""
    pos += _float32(1, float(vehicle["lat"]))   # latitude
    pos += _float32(2, float(vehicle["lon"]))   # longitude

    if vehicle.get("bearing") is not None:
        pos += _float32(3, float(vehicle["bearing"]))

    if vehicle.get("speed") is not None:
        # GTFS-RT speed w m/s; jeśli source podaje km/h – przelicz tutaj
        pos += _float32(4, float(vehicle["speed"]))

    return pos


def _build_trip_descriptor(trip_id) -> bytes:
    return _str(1, str(trip_id))


def _build_vehicle_descriptor(vehicle: dict, fallback_id: str) -> bytes:
    vd = b""
    vid = str(vehicle.get("id") or vehicle.get("vehicle_id") or fallback_id)
    vd += _str(1, vid)                          # id

    label = str(vehicle.get("label") or vehicle.get("name") or "")
    if label:
        vd += _str(2, label)                    # label

    return vd


def _build_vehicle_position(entity_id: str, trip_id: str, vehicle: dict) -> bytes:
    vp = b""
    vp += _msg(1, _build_trip_descriptor(trip_id))          # trip
    vp += _msg(2, _build_position(vehicle))                 # position
    vp += _msg(3, _build_vehicle_descriptor(vehicle, entity_id))  # vehicle
    vp += _enum(5, 2)                                       # IN_TRANSIT_TO (domyślnie)
    return vp


def _build_feed_entity(entity_id: str, trip_id: str, vehicle: dict) -> bytes:
    fe = b""
    fe += _str(1, entity_id)                                # id
    fe += _msg(4, _build_vehicle_position(entity_id, trip_id, vehicle))
    return fe


def build_gtfs_rt_pb(gtfs_json: list) -> bytes:
    """
    Buduje binarny feed GTFS-Realtime (FeedMessage) z listy pojazdów.

    Każdy element gtfs_json powinien mieć pola:
      - id         (str)  unikalny identyfikator encji
      - trip_id    (str)
      - vehicle    (dict) z kluczami: lat, lon
                          opcjonalne: bearing, speed, id, label
    """
    timestamp = int(datetime.utcnow().timestamp())

    # FeedHeader
    header = b""
    header += _str(1, "2.0")        # gtfs_realtime_version
    header += _uint64(2, timestamp) # timestamp

    feed = _msg(1, header)

    for v in gtfs_json:
        entity_bytes = _build_feed_entity(
            entity_id=v["id"],
            trip_id=v["trip_id"],
            vehicle=v["vehicle"],
        )
        feed += _msg(2, entity_bytes)

    return feed


# ============================
# GTFS STATIC (ZIP → stops.txt)
# ============================
async def load_stops_from_gtfs(gtfs_source: str, miasto: str) -> list[str]:
    zip_path = os.path.join(GTFS_CACHE_DIR, f"{miasto}.zip")

    if gtfs_source.startswith("http"):
        async with aiohttp.ClientSession() as session:
            async with session.get(gtfs_source) as resp:
                content = await resp.read()
                with open(zip_path, "wb") as f:
                    f.write(content)
    else:
        zip_path = gtfs_source

    stops = []
    with zipfile.ZipFile(zip_path) as z:
        with z.open("stops.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
            for row in reader:
                stops.append(row["stop_id"])
    return stops

# ============================
# API
# ============================
async def fetch_departures(session, miasto_url, stop_id):
    url = f"https://{miasto_url}.kiedyprzyjedzie.pl/api/departures/{stop_id}"
    try:
        async with session.get(url) as resp:
            data = await resp.json()
            return [
                {
                    "trip_id": r["trip_id"],
                    "trip_execution_id": r["trip_execution_id"],
                    "time": r["time"],
                }
                for r in data.get("rows", [])
            ]
    except:
        return []

async def fetch_trip_execution(session, miasto_url, trip_execution_id):
    base64_id = encode_trip_execution(trip_execution_id)
    url = f"https://{miasto_url}.kiedyprzyjedzie.pl/api/trip_execution/{base64_id}/0"
    try:
        async with session.get(url) as resp:
            data = await resp.json()
            vehicle = data.get("vehicle")
            if vehicle and vehicle.get("lat") is not None:
                return {
                    "trip_execution_id": trip_execution_id,
                    "trip": data.get("trip"),
                    "vehicle": vehicle,
                }
    except:
        return None

# ============================
# OBSŁUGA JEDNEGO MIASTA
# ============================
async def process_city(miasto_cfg):
    nazwa = miasto_cfg["nazwa"]
    url_miasto = miasto_cfg["url_miasto"]
    format_rt = miasto_cfg["realtime_format"]

    stops = await load_stops_from_gtfs(miasto_cfg["gtfs_source"], nazwa)

    async with aiohttp.ClientSession() as session:
        departures_tasks = [
            fetch_departures(session, url_miasto, stop_id)
            for stop_id in stops
        ]
        departures_results = await asyncio.gather(*departures_tasks)

        all_departures = [d for sub in departures_results for d in sub]

        trip_map = {}
        for dep in all_departures:
            te_id = dep["trip_execution_id"]
            dep_time = parse_time_string(dep["time"])
            if te_id not in trip_map or dep_time < trip_map[te_id]["time"]:
                trip_map[te_id] = {
                    "trip_id": dep["trip_id"],
                    "time": dep_time,
                }

        trip_exec_tasks = [
            fetch_trip_execution(session, url_miasto, te_id)
            for te_id in trip_map
        ]
        trip_execs = await asyncio.gather(*trip_exec_tasks)
        valid = [t for t in trip_execs if t]

        gtfs_json = []
        seen = {}

        for t in valid:
            te_id = t["trip_execution_id"]
            full_id = f"{nazwa}_{te_id.replace(':', '_')}"

            vehicle = t["vehicle"]
            lat = vehicle.get("lat")
            lon = vehicle.get("lon")

            # prefiks = wszystko oprócz ostatniego "_X"
            id_parts = full_id.split("_")
            id_prefix = "_".join(id_parts[:-1])

            dedup_key = (id_prefix, lat, lon)

            # jeśli już mamy taki pojazd → pomijamy
            if dedup_key in seen:
                continue

            seen[dedup_key] = True

            gtfs_json.append({
                "id": full_id,
                "trip_id": trip_map[te_id]["trip_id"],
                "route": t["trip"]["line"]["name"] if t.get("trip") else None,
                "vehicle": vehicle,
            })

        if format_rt in ("json", "both"):
            with open(f"{nazwa}_gtfs_rt.json", "w", encoding="utf-8") as f:
                json.dump(gtfs_json, f, ensure_ascii=False, indent=2)
            print(f"[{nazwa}] JSON zapisany ({len(gtfs_json)} pojazdów)")

        if format_rt in ("pb", "both"):
            pb_data = build_gtfs_rt_pb(gtfs_json)
            with open(f"{nazwa}_gtfs_rt.pb", "wb") as f:
                f.write(pb_data)
            print(f"[{nazwa}] PB zapisany ({len(pb_data)} bajtów, {len(gtfs_json)} pojazdów)")

        print(f"[{nazwa}] RT zaktualizowany ({len(gtfs_json)} pojazdów)")

# ============================
# SERWER HTTP (port 8050)
# ============================
# Dostępne endpointy:
#   GET /                        → lista dostępnych feedów (HTML)
#   GET /{miasto}_gtfs_rt.pb     → plik .pb dla danego miasta
#   GET /{miasto}_gtfs_rt.json   → plik .json dla danego miasta (jeśli istnieje)

async def handle_pb(request: aiohttp.web.Request) -> aiohttp.web.Response:
    filename = request.match_info["filename"]
    filepath = filename  # pliki zapisywane są w katalogu roboczym

    if not os.path.isfile(filepath):
        raise aiohttp.web.HTTPNotFound(reason=f"Plik '{filename}' nie istnieje lub nie został jeszcze wygenerowany.")

    with open(filepath, "rb") as f:
        data = f.read()

    # Zgodnie ze specyfikacją GTFS-RT typ MIME to application/x-protobuf
    return aiohttp.web.Response(
        body=data,
        content_type="application/x-protobuf",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-cache",
        },
    )

async def handle_json(request: aiohttp.web.Request) -> aiohttp.web.Response:
    filename = request.match_info["filename"]
    filepath = filename

    if not os.path.isfile(filepath):
        raise aiohttp.web.HTTPNotFound(reason=f"Plik '{filename}' nie istnieje lub nie został jeszcze wygenerowany.")

    with open(filepath, "r", encoding="utf-8") as f:
        data = f.read()

    return aiohttp.web.Response(
        text=data,
        content_type="application/json",
        headers={"Cache-Control": "no-cache"},
    )

async def handle_index(request: aiohttp.web.Request) -> aiohttp.web.Response:
    rows = ""
    for m in MIASTA:
        nazwa = m["nazwa"]
        fmt = m["realtime_format"]

        pb_file   = f"{nazwa}_gtfs_rt.pb"
        json_file = f"{nazwa}_gtfs_rt.json"

        pb_exists   = os.path.isfile(pb_file)
        json_exists = os.path.isfile(json_file)

        pb_link   = f'<a href="/{pb_file}">{pb_file}</a>' if pb_exists else f'<span style="color:#aaa">{pb_file} (oczekiwanie…)</span>'
        json_link = f'<a href="/{json_file}">{json_file}</a>' if json_exists else f'<span style="color:#aaa">{json_file} (oczekiwanie…)</span>'

        feeds = []
        if fmt in ("pb", "both"):
            feeds.append(pb_link)
        if fmt in ("json", "both"):
            feeds.append(json_link)

        rows += f"<tr><td>{nazwa}</td><td>{'<br>'.join(feeds)}</td></tr>"

    html = f"""<!DOCTYPE html>
<html lang="pl">
<head>
  <meta charset="utf-8">
  <title>GTFS-RT Feed Server</title>
  <style>
    body {{ font-family: monospace; padding: 2rem; background: #111; color: #eee; }}
    h1   {{ color: #7cf; }}
    table {{ border-collapse: collapse; margin-top: 1rem; }}
    th, td {{ padding: .4rem 1rem; border: 1px solid #444; text-align: left; }}
    th {{ background: #222; color: #7cf; }}
    a  {{ color: #aef; }}
    small {{ color: #666; }}
  </style>
</head>
<body>
  <h1>GTFS-RT Feed Server</h1>
  <small>Odświeżanie co {POLL_INTERVAL}s · {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</small>
  <table>
    <thead><tr><th>Miasto</th><th>Feed</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>
</body>
</html>"""

    return aiohttp.web.Response(text=html, content_type="text/html")


async def start_http_server(app: aiohttp.web.Application) -> None:
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    site = aiohttp.web.TCPSite(runner, "0.0.0.0", 8050)
    await site.start()
    print(f"[HTTP] Serwer GTFS-RT działa na http://localhost:8050/")


def build_app() -> aiohttp.web.Application:
    app = aiohttp.web.Application()
    app.router.add_get("/",                   handle_index)
    app.router.add_get("/{filename:.+\\.pb}", handle_pb)
    app.router.add_get("/{filename:.+\\.json}", handle_json)
    return app


# ============================
# MAIN LOOP
# ============================
async def main():
    app = build_app()
    await start_http_server(app)

    while True:
        await asyncio.gather(*(process_city(m) for m in MIASTA))
        await asyncio.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())