import os
import time
import json
import math
import zipfile
import shutil
import requests
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta
from google.transit import gtfs_realtime_pb2

# =========================
# KONFIGURACJA
# =========================

CITY = "bialystok"
FEED_NAME = "BKM"

CESIP_TRACES_URL = "https://przystanki.bialystok.pl/csip/ext_channel/traces.json"
BLOCKS_API_URL = "http://127.0.0.1:8000/api/getBlocksForFeedAndDate.json"

GTFS_STATIC_ZIP_URL = "http://127.0.0.1:8050/ontimegtfs/bialystok.zip"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "gtfs_static")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

REFRESH_INTERVAL = 10  # sekundy
HTTP_PORT = 8060
HTTP_PATH_PREFIX = "/bialystokrealtime"

# =========================
# ŁĄCZENIE BRYGAD W BLOKI
# =========================
# Mapowanie brygad na połączone block_id, osobno dla każdego service_id.
# Porównanie service_id jest po prefiksie: "P-0", "P-2" itp. traktowane jako "P".

BLOCK_ID_MERGES = [
    {
        "service_id": "P",
        "brigades": ["009-01", "126-01"],
        "block_id": "009-01+126-01",
    },
    {
        "service_id": "P",
        "brigades": ["102-01", "122-01", "132-01"],
        "block_id": "102-01+122-01+132-01",
    },
    {
        "service_id": "P",
        "brigades": ["102-02", "122-02", "132-02"],
        "block_id": "102-02+122-02+132-02",
    },
    {
        "service_id": "P",
        "brigades": ["102-03", "122-03", "132-03"],
        "block_id": "102-03+122-03+132-03",
    },
    {
        "service_id": "P",
        "brigades": ["111-02", "111-03"],
        "block_id": "111-02+111-03",
    },
    {
        "service_id": "R",
        "brigades": ["102-01", "132-01", "132-02", "102-02"],
        "block_id": "102-01+132-01+132-02+102-02",
    },
    {
        "service_id": "R",
        "brigades": ["108-01", "108-02"],
        "block_id": "108-01+108-02",
    },
    {
        "service_id": "R",
        "brigades": ["111-01", "111-02", "111-03", "111-04"],
        "block_id": "111-01+111-02+111-03+111-04",
    },
    {
        "service_id": "S",
        "brigades": ["111-01", "111-02"],
        "block_id": "111-01+111-02",
    },
    {
        "service_id": "S",
        "brigades": ["108-01", "108-02"],
        "block_id": "108-01+108-02",
    },
]

# =========================
# UTILITY
# =========================

def ensure_dirs():
    os.makedirs(STATIC_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2

    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def time_to_seconds(t):
    h, m, s = map(int, t.split(":"))
    return h * 3600 + m * 60 + s


# =========================
# GTFS STATIC
# =========================

GTFS_STOPS = {}
GTFS_STOP_TIMES = {}

# Bazowy service_id obowiązujący dzisiaj, np. "P", "R", "S"
TODAY_BASE_SERVICE_ID: str | None = None


def download_and_extract_gtfs():
    print("🔄 Pobieranie GTFS-Static...")
    r = requests.get(GTFS_STATIC_ZIP_URL, timeout=60)
    zip_path = os.path.join(BASE_DIR, "gtfs.zip")

    with open(zip_path, "wb") as f:
        f.write(r.content)

    if os.path.exists(STATIC_DIR):
        shutil.rmtree(STATIC_DIR)

    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(STATIC_DIR)

    os.remove(zip_path)
    print("✅ GTFS-Static pobrany i rozpakowany")

    load_gtfs_static()


def load_gtfs_static():
    global GTFS_STOPS, GTFS_STOP_TIMES

    print("📚 Wczytywanie GTFS-Static...")

    GTFS_STOPS.clear()
    GTFS_STOP_TIMES.clear()

    with open(os.path.join(STATIC_DIR, "stops.txt"), encoding="utf-8") as f:
        next(f)
        for line in f:
            stop_id, _, _, lat, lon, *_ = line.strip().split(",")
            GTFS_STOPS[stop_id] = (float(lat), float(lon))

    with open(os.path.join(STATIC_DIR, "stop_times.txt"), encoding="utf-8") as f:
        next(f)
        for line in f:
            trip_id, arr, dep, stop_id, seq, *_ = line.strip().split(",")
            GTFS_STOP_TIMES.setdefault(trip_id, []).append({
                "stop_id": stop_id,
                "arrival": arr,
                "departure": dep,
                "seq": int(seq)
            })

    print(f"✅ Stops: {len(GTFS_STOPS)} | StopTimes trips: {len(GTFS_STOP_TIMES)}")

    # Po wczytaniu staticu od razu ustalamy dzisiejszy service_id
    load_today_service_id()


def load_today_service_id():
    """
    Wczytuje calendar_dates.txt i ustala bazowy service_id dla dzisiejszej daty.
    Np. "P-2" -> "P", "R-0" -> "R", "S-1" -> "S".
    """
    global TODAY_BASE_SERVICE_ID

    today = datetime.now().strftime("%Y%m%d")
    path = os.path.join(STATIC_DIR, "calendar_dates.txt")

    try:
        with open(path, encoding="utf-8") as f:
            next(f)  # pomiń nagłówek
            for line in f:
                parts = line.strip().split(",")
                if len(parts) < 3:
                    continue
                service_id, date, exception_type = parts[0], parts[1], parts[2]
                if date == today and exception_type == "1":
                    # Bierzemy tylko część przed pierwszym "-", np. "P-2" -> "P"
                    TODAY_BASE_SERVICE_ID = service_id.split("-")[0]
                    print(
                        f"📅 Dzisiejszy service_id: '{service_id}' "
                        f"-> baza: '{TODAY_BASE_SERVICE_ID}'"
                    )
                    return

        print(f"⚠️ Nie znaleziono service_id dla daty {today} w calendar_dates.txt")
        TODAY_BASE_SERVICE_ID = None

    except FileNotFoundError:
        print("❌ Brak pliku calendar_dates.txt w GTFS-Static")
        TODAY_BASE_SERVICE_ID = None
    except Exception as e:
        print(f"❌ Błąd wczytywania calendar_dates.txt: {e}")
        TODAY_BASE_SERVICE_ID = None


def schedule_midnight_refresh():
    def loop():
        while True:
            now = datetime.now()
            tomorrow = datetime.combine(
                now.date() + timedelta(days=1), datetime.min.time()
            )
            time.sleep((tomorrow - now).total_seconds())
            download_and_extract_gtfs()   # wywołuje też load_today_service_id()
            fetch_and_cache_blocks()

    threading.Thread(target=loop, daemon=True).start()


# =========================
# SAFE FETCH
# =========================

def fetch_traces_safe():
    print("🌐 Pobieranie traces.json...")
    try:
        r = requests.get(CESIP_TRACES_URL, timeout=10)

        if r.status_code != 200:
            print(f"❌ HTTP {r.status_code} z CeSIP")
            return []

        if not r.text.strip():
            print("⚠️ traces.json pusty response")
            return []

        data = r.json()

        if not isinstance(data, dict):
            print("❌ Nieoczekiwany typ JSON (nie dict)")
            return []

        traces = data.get("traces")
        if not isinstance(traces, list):
            print("❌ Brak pola 'traces' albo nie jest listą")
            return []

        print(
            f"📦 Metadata: date={data.get('date')} "
            f"reportVersion={data.get('reportVersion')}"
        )
        return traces

    except json.JSONDecodeError:
        print("❌ traces.json NIE JEST JSON-em")
        print(r.text[:500])
        return []

    except Exception as e:
        print(f"❌ Błąd pobierania traces.json: {e}")
        return []


# =========================
# BLOCKS CACHE
# =========================

# Mapa: block_id (str) -> lista kursów [{"trip_id": ..., "route_id": ..., ...}, ...]
BLOCKS_CACHE: dict = {}


def fetch_and_cache_blocks():
    """Pobiera wszystkie bloki dla bieżącej daty jednym requestem i cache'uje wynik."""
    global BLOCKS_CACHE

    today = datetime.now().strftime("%Y%m%d")
    print(f"🔄 Pobieranie bloków (getBlocksForFeedAndDate) dla daty {today}...")

    try:
        r = requests.get(
            BLOCKS_API_URL,
            params={"city": CITY, "feed_name": FEED_NAME, "date": today},
            timeout=15
        )

        if r.status_code != 200:
            print(f"❌ API error HTTP {r.status_code}: {r.text[:200]}")
            return

        data = r.json()
        blocks = data.get("blocks")

        if not isinstance(blocks, dict):
            print("❌ Odpowiedź nie zawiera poprawnego pola 'blocks'")
            return

        BLOCKS_CACHE = blocks
        print(f"✅ Załadowano {len(BLOCKS_CACHE)} brygad z API")

    except Exception as e:
        print(f"❌ Błąd pobierania bloków: {e}")


def resolve_block_id(brigade: str) -> str:
    """
    Zwraca efektywny block_id dla danej brygady, uwzględniając BLOCK_ID_MERGES
    dla dzisiejszego bazowego service_id.

    Jeśli brygada należy do grupy scalania pasującej do TODAY_BASE_SERVICE_ID,
    zwraca połączony block_id (np. "009-01+126-01").
    W przeciwnym razie zwraca oryginalną brygadę jako block_id.
    """
    if TODAY_BASE_SERVICE_ID is None:
        return brigade

    for merge in BLOCK_ID_MERGES:
        if merge["service_id"] == TODAY_BASE_SERVICE_ID and brigade in merge["brigades"]:
            merged = merge["block_id"]
            print(
                f"🔗 Brygada '{brigade}' -> połączony block_id '{merged}' "
                f"(service_id='{TODAY_BASE_SERVICE_ID}')"
            )
            return merged

    return brigade


def get_course_from_cache(brigade, index):
    """
    Zwraca kurs dla danej brygady i indeksu z lokalnego cache'u.
    Uwzględnia łączenie brygad w bloki na podstawie BLOCK_ID_MERGES.
    """
    brigade_str = str(brigade)
    block_id = resolve_block_id(brigade_str)

    courses = BLOCKS_CACHE.get(block_id)

    # Fallback: jeśli połączony block_id nie istnieje w cache, spróbuj oryginalnej brygady
    if not courses and block_id != brigade_str:
        print(
            f"⚠️ Połączony block_id '{block_id}' nie w cache — "
            f"próbuję oryginalnej brygady '{brigade_str}'"
        )
        courses = BLOCKS_CACHE.get(brigade_str)
        if courses:
            block_id = brigade_str  # dla czytelności logów poniżej

    if not courses:
        print(f"⚠️ Brygada '{brigade_str}' (block_id='{block_id}') nie w cache")
        return None

    if index >= len(courses):
        print(
            f"⚠️ courseBrigadeIndex={index} poza zakresem dla "
            f"block_id='{block_id}' (len={len(courses)})"
        )
        return None

    return courses[index]


# =========================
# HTTP SERVER
# =========================

def start_http_server():
    class RealtimeHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if not self.path.startswith(HTTP_PATH_PREFIX + "/"):
                self.send_error(404, "Not Found")
                return

            filename = self.path[len(HTTP_PATH_PREFIX) + 1:].split("?")[0]

            ALLOWED_FILES = {
                "vehicle_positions.pb":   "application/octet-stream",
                "trip_updates.pb":        "application/octet-stream",
                "vehicle_positions.json": "application/json",
                "trip_updates.json":      "application/json",
            }

            if filename not in ALLOWED_FILES:
                self.send_error(404, f"File '{filename}' not found")
                return

            filepath = os.path.join(OUTPUT_DIR, filename)
            if not os.path.isfile(filepath):
                self.send_error(503, "File not yet generated")
                return

            content_type = ALLOWED_FILES[filename]
            with open(filepath, "rb") as f:
                data = f.read()

            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, format, *args):
            pass  # wyciszamy logi HTTP

    server = HTTPServer(("127.0.0.1", HTTP_PORT), RealtimeHandler)
    print(
        f"🌍 HTTP server działa na "
        f"http://127.0.0.1:{HTTP_PORT}{HTTP_PATH_PREFIX}/"
    )
    threading.Thread(target=server.serve_forever, daemon=True).start()


# =========================
# MAIN LOOP
# =========================

def main():
    ensure_dirs()
    start_http_server()
    download_and_extract_gtfs()   # wewnątrz wywołuje load_gtfs_static() -> load_today_service_id()
    fetch_and_cache_blocks()
    schedule_midnight_refresh()

    while True:
        print("\n==============================")
        print(f"⏱️  TICK {datetime.now().strftime('%H:%M:%S')}")
        print(f"📅 service_id baza: {TODAY_BASE_SERVICE_ID}")
        print("==============================")

        traces = fetch_traces_safe()
        print(f"📡 traces count: {len(traces)}")

        feed_vp = gtfs_realtime_pb2.FeedMessage()
        feed_tu = gtfs_realtime_pb2.FeedMessage()

        now_ts = int(time.time())

        feed_vp.header.gtfs_realtime_version = "2.0"
        feed_vp.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
        feed_vp.header.timestamp = now_ts

        feed_tu.header.gtfs_realtime_version = "2.0"
        feed_tu.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
        feed_tu.header.timestamp = now_ts

        vp_json = {
            "header": {
                "gtfs_realtime_version": "2.0",
                "timestamp": now_ts
            },
            "entity": []
        }
        tu_json = []

        for t in traces:
            print("\n🚍 POJAZD RAW:")
            print(json.dumps(t, indent=2, ensure_ascii=False))

            brigade = t.get("brigade")
            index = t.get("courseBrigadeIndex")

            trip_id = None
            route_id = None

            if brigade is None or index is None:
                print("⚠️ brak brigade / courseBrigadeIndex — pojazd bez kursu")
            else:
                course = get_course_from_cache(brigade, index)
                if not course:
                    print("⚠️ brak kursu w cache — pojazd bez kursu")
                else:
                    trip_id = course["trip_id"]
                    route_id = course["route_id"]
                    print(f"➡️ trip_id={trip_id} route_id={route_id}")

            # VEHICLE POSITION (zapisujemy zawsze, niezależnie od trip_id)
            e = feed_vp.entity.add()
            e.id = f"VP_{t['vehicleNo']}"

            vp = e.vehicle
            vp.vehicle.id = str(t["vehicleNo"])
            vp.vehicle.label = str(t["vehicleNo"])
            if trip_id:
                vp.trip.trip_id = trip_id
            if route_id:
                vp.trip.route_id = route_id
            vp.position.latitude = t["lat"]
            vp.position.longitude = t["lon"]
            vp.position.speed = (t.get("speed") or 0) / 3.6
            vp.timestamp = now_ts

            entity = {
                "id": str(t["vehicleNo"]),
                "vehicle": {
                    "vehicle": {
                        "id": str(t["vehicleNo"]),
                        "label": str(t["vehicleNo"])
                    },
                    "position": {
                        "latitude": t["lat"],
                        "longitude": t["lon"],
                        "speed": (t.get("speed") or 0)
                    },
                    "timestamp": now_ts
                }
            }
            if trip_id or route_id:
                entity["vehicle"]["trip"] = {}
                if trip_id:
                    entity["vehicle"]["trip"]["trip_id"] = trip_id
                if route_id:
                    entity["vehicle"]["trip"]["route_id"] = route_id
            vp_json["entity"].append(entity)

            if not trip_id:
                continue

            # TRIP UPDATE
            stops = GTFS_STOP_TIMES.get(trip_id)
            if not stops:
                print("⚠️ brak stop_times")
                continue

            next_stop = stops[0]
            stop_lat, stop_lon = GTFS_STOPS[next_stop["stop_id"]]

            dist = haversine(t["lat"], t["lon"], stop_lat, stop_lon)
            now_sec = (
                datetime.now().hour * 3600
                + datetime.now().minute * 60
                + datetime.now().second
            )
            sched = time_to_seconds(next_stop["arrival"])

            speed_mps = max((t.get("speed") or 10) / 3.6, 1)
            eta = dist / speed_mps
            delay = int(now_sec + eta - sched)

            print(
                f"🛑 next_stop={next_stop['stop_id']} "
                f"dist={int(dist)}m delay={delay}s"
            )

            e2 = feed_tu.entity.add()
            e2.id = f"TU_{trip_id}"

            tu = e2.trip_update
            tu.trip.trip_id = trip_id
            tu.trip.route_id = route_id
            tu.timestamp = now_ts

            stu = tu.stop_time_update.add()
            stu.stop_id = next_stop["stop_id"]
            stu.stop_sequence = next_stop["seq"]
            stu.arrival.delay = delay
            stu.departure.delay = delay

            tu_json.append(trip_id)

        with open(os.path.join(OUTPUT_DIR, "vehicle_positions.pb"), "wb") as f:
            f.write(feed_vp.SerializeToString())

        with open(os.path.join(OUTPUT_DIR, "trip_updates.pb"), "wb") as f:
            f.write(feed_tu.SerializeToString())

        with open(os.path.join(OUTPUT_DIR, "vehicle_positions.json"), "w", encoding="utf-8") as f:
            json.dump(vp_json, f, indent=2)

        with open(os.path.join(OUTPUT_DIR, "trip_updates.json"), "w", encoding="utf-8") as f:
            json.dump(tu_json, f, indent=2)

        print("💾 GTFS-RT zapisane")
        time.sleep(REFRESH_INTERVAL)


if __name__ == "__main__":
    main()