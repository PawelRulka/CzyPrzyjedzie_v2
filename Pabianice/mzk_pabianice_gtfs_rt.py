"""
MZK Pabianice – GTFS-Realtime VehiclePosition
===============================================
Co 30 sekund pobiera pozycje pojazdow ze wszystkich linii,
buduje plik gtfs_rt_output/vehicle_positions.pb (protobuf)
oraz serwuje go przez HTTP na porcie 8091.

Wymagania:
    pip install requests gtfs-realtime-bindings protobuf

Uruchomienie (po wcześniejszym wygenerowaniu GTFS-Static):
    python mzk_pabianice_gtfs_rt.py

Plik dostępny pod: http://localhost:8091/vehicle_positions.pb

────────────────────────────────────────────────────────
Mapowanie trip_id (skrócone → pełne)
────────────────────────────────────────────────────────
CNR_GetVehicles zwraca skrócone ID kursu, np. 669.
W GTFS-Static trip_id ma postać 185xxxx lub 186xxxx, np. 1859669.
Prefix (1859, 1860, …) może się zmieniać między rozkładami.

Rozwiązanie: przy starcie wczytujemy trips.txt i budujemy mapę:
    str(short_id) → full_trip_id

Aby uniknąć kolizji (dwa różne pełne ID z tym samym suffixem),
preferujemy kurs przypisany do dzisiejszej daty (service_id =
"DATE_YYYYMMDD" dla dzisiejszej daty). Jeśli dla danego suffixu
nie ma kursu na dziś, wybieramy pierwszy znaleziony.
"""

import csv
import http.server
import math
import os
import threading
import time
from datetime import date
from xml.etree import ElementTree as ET

import requests
from google.transit import gtfs_realtime_pb2

# ---------------------------------------------------------------------------
# Konfiguracja
# ---------------------------------------------------------------------------

BASE_URL    = "https://komunikacjapabianice.pl/rj"

# Ścieżka do trips.txt wygenerowanego przez GTFS-Static
STATIC_DIR  = "gtfs_output"
TRIPS_FILE  = os.path.join(STATIC_DIR, "trips.txt")

OUTPUT_DIR  = "gtfs_rt_output"
OUTPUT_FILE = "vehicle_positions.pb"
HTTP_PORT   = 8091

POLL_INTERVAL_SECONDS = 30

LINES = ["1", "2", "3", "4", "5", "6", "7",
         "260", "261", "262", "263", "265", "A41", "T"]

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "GTFS-RT-scraper/1.0 (badania akademickie)"
})
SESSION.verify = False  # wyłącz weryfikację SSL (problem z cert. pośrednim na Windowsie)


# ---------------------------------------------------------------------------
# Mapa suffix → full_trip_id
# ---------------------------------------------------------------------------

def build_trip_id_map(trips_file: str) -> dict[str, str]:
    """
    Wczytuje trips.txt i buduje mapę:
        short_key (str) → full_trip_id (str)

    short_key to str(short_id) zwracany przez CNR_GetVehicles, np. "653".
    full_trip_id to pełne ID z GTFS-Static, np. "1859653" lub "1860653".

    Relacja: full_trip_id.endswith(short_key) oraz prefix jest czysto numeryczny.
    Dzięki temu działa dla dowolnego prefixu (1859, 1860, 1861, ...) bez
    zakładania stałej długości lub wartości prefixu.

    Przy kolizji (dwa full_id kończące się tym samym short_key):
      - Preferuj kurs z dzisiejszą datą (service_id = "DATE_YYYYMMDD").
      - Jeśli żaden lub oba pasują do dziś → pierwszy znaleziony.
    """
    today_service = f"DATE_{date.today().strftime('%Y%m%d')}"

    # short_key -> (full_trip_id, is_today)
    best: dict[str, tuple[str, bool]] = {}

    if not os.path.exists(trips_file):
        print(f"  [WARN] Nie znaleziono {trips_file} – mapowanie trip_id niedostepne.")
        return {}

    with open(trips_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            full_id = row.get("trip_id", "").strip()
            if not full_id.isdigit():
                continue

            is_today = row.get("service_id", "") == today_service

            # Indeksujemy po WSZYSTKICH mozliwych suffixach od 1 do len-1 cyfr.
            # Np. "1859653" tworzy klucze: "3","53","653","9653","59653","859653".
            # CNR zwraca zwykle 3-4 cyfrowe ID, wiec kluczowe sa krotkie suffiksy.
            for suffix_len in range(1, len(full_id)):
                short_key = full_id[-suffix_len:]
                existing  = best.get(short_key)
                if existing is None:
                    best[short_key] = (full_id, is_today)
                elif is_today and not existing[1]:
                    # Dzisiejszy kurs ma priorytet
                    best[short_key] = (full_id, is_today)

    result = {sk: fid for sk, (fid, _) in best.items()}
    today_count = sum(1 for sk, (fid, it) in best.items() if it)
    print(f"  [TripMap] Zaladowano {len(result)} kluczy z {trips_file} "
          f"(dzis: {today_count} kluczy, service_id: {today_service})")
    return result


# Globalna mapa – odświeżana przy każdym nowym dniu
_trip_id_map:    dict[str, str] = {}
_trip_map_date:  date | None    = None
_trip_map_lock   = threading.Lock()

RELOAD_INTERVAL_SEC = 3600  # odświeżaj mapę co godzinę


def get_trip_id_map() -> dict[str, str]:
    """Zwraca aktualną mapę; przeładowuje jeśli zmieniła się data."""
    global _trip_id_map, _trip_map_date
    today = date.today()
    with _trip_map_lock:
        if _trip_map_date != today:
            _trip_id_map   = build_trip_id_map(TRIPS_FILE)
            _trip_map_date = today
    return _trip_id_map


def resolve_trip_id(short_id: int) -> str:
    """
    Zamienia skrócone ID kursu z CNR_GetVehicles na pełne trip_id z GTFS.

    Szuka w mapie po kluczu str(short_id), np. "653" → "1859653".
    Działa dla dowolnego prefixu (1859xxx, 1860xxx, 1861xxx itd.).

    Jeśli nie znaleziono dopasowania, zwraca oryginalną wartość jako string
    – feed pozostaje czytelny, ale vehicle nie będzie dopasowany do tripu.
    """
    trip_map  = get_trip_id_map()
    short_key = str(short_id)
    full_id   = trip_map.get(short_key)
    if full_id is None:
        return short_key  # fallback – bez dopasowania
    return full_id


# ---------------------------------------------------------------------------
# Pomocnicze HTTP / XML
# ---------------------------------------------------------------------------

def get_xml(url: str) -> ET.Element | None:
    try:
        r = SESSION.get(url, timeout=10)
        r.raise_for_status()
        return ET.fromstring(r.text)
    except Exception as e:
        print(f"  [WARN] {url}: {e}")
        return None


def bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    d_lon = math.radians(lon2 - lon1)
    lat1r = math.radians(lat1)
    lat2r = math.radians(lat2)
    x = math.sin(d_lon) * math.cos(lat2r)
    y = (math.cos(lat1r) * math.sin(lat2r)
         - math.sin(lat1r) * math.cos(lat2r) * math.cos(d_lon))
    b = math.degrees(math.atan2(x, y))
    return (b + 360) % 360


# ---------------------------------------------------------------------------
# Parsowanie CNR_GetVehicles
# ---------------------------------------------------------------------------

def parse_vehicle_list(xml_root: ET.Element) -> list[dict]:
    vehicles = []

    for p_elem in xml_root.findall("p"):
        raw = p_elem.text.strip() if p_elem.text else ""
        if not raw:
            continue
        try:
            data = _parse_array(raw)
        except Exception as e:
            print(f"  [WARN] parse array: {e}  raw={raw[:80]}")
            continue

        if len(data) < 27:
            continue

        try:
            vehicle_id    = str(data[0])
            vehicle_label = str(data[1])
            short_trip_id = int(data[5])         # skrócone ID kursu
            lon_current   = float(data[9])
            lat_current   = float(data[10])
            lon_prev      = float(data[11])
            lat_prev      = float(data[12])
            delay_sec     = int(data[13]) if str(data[13]).lstrip("-").isdigit() else 0
            headsign      = str(data[26]).strip()

            if (lat_current, lon_current) != (lat_prev, lon_prev):
                hdg = bearing(lat_prev, lon_prev, lat_current, lon_current)
            else:
                hdg = 0.0

            # Rozwiąż pełne trip_id z mapy zbudowanej na podstawie trips.txt
            full_trip_id = resolve_trip_id(short_trip_id)

            vehicles.append({
                "vehicle_id":    vehicle_id,
                "vehicle_label": vehicle_label,
                "trip_id":       full_trip_id,
                "lat":           lat_current,
                "lon":           lon_current,
                "bearing":       hdg,
                "delay_sec":     delay_sec,
                "headsign":      headsign,
            })
        except (IndexError, ValueError, TypeError) as e:
            print(f"  [WARN] field extraction: {e}")

    return vehicles


def _parse_array(text: str) -> list:
    import ast
    return ast.literal_eval(text)


# ---------------------------------------------------------------------------
# Pobieranie pojazdów ze wszystkich linii
# ---------------------------------------------------------------------------

def fetch_all_vehicles() -> list[dict]:
    all_vehicles: dict[str, dict] = {}

    for line in LINES:
        url  = f"{BASE_URL}/Routes/CNR_GetVehicles?r={line}&d=&nb=&krs="
        root = get_xml(url)
        if root is None:
            continue

        for v in parse_vehicle_list(root):
            vid = v["vehicle_id"]
            if vid not in all_vehicles:
                all_vehicles[vid] = v

    return list(all_vehicles.values())


# ---------------------------------------------------------------------------
# Budowanie feed GTFS-RT
# ---------------------------------------------------------------------------

def build_feed(vehicles: list[dict]) -> bytes:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = int(time.time())

    for v in vehicles:
        entity    = feed.entity.add()
        entity.id = v["vehicle_id"]

        vp = entity.vehicle

        vp.trip.trip_id = v["trip_id"]

        vp.vehicle.id    = v["vehicle_id"]
        vp.vehicle.label = v["vehicle_label"]

        vp.position.latitude  = v["lat"]
        vp.position.longitude = v["lon"]
        vp.position.bearing   = v["bearing"]

    return feed.SerializeToString()


# ---------------------------------------------------------------------------
# Pętla pollingu
# ---------------------------------------------------------------------------

_latest_pb: bytes = b""
_pb_lock = threading.Lock()


def polling_loop():
    global _latest_pb

    print(f"  Polling co {POLL_INTERVAL_SECONDS}s dla {len(LINES)} linii...")

    while True:
        start = time.time()
        try:
            vehicles = fetch_all_vehicles()
            pb_bytes = build_feed(vehicles)

            with _pb_lock:
                _latest_pb = pb_bytes

            out_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
            with open(out_path, "wb") as f:
                f.write(pb_bytes)

            matched   = sum(1 for v in vehicles if not v["trip_id"].isdigit()
                            or len(v["trip_id"]) > 4)
            unmatched = len(vehicles) - matched
            print(f"  [{_ts()}] {len(vehicles)} pojazdów "
                  f"(dopasowanych: {matched}, bez dopasowania: {unmatched})")

        except Exception as e:
            print(f"  [{_ts()}] [ERR] polling: {e}")

        elapsed  = time.time() - start
        sleep_for = max(0, POLL_INTERVAL_SECONDS - elapsed)
        time.sleep(sleep_for)


def _ts() -> str:
    return time.strftime("%H:%M:%S")


# ---------------------------------------------------------------------------
# Serwer HTTP
# ---------------------------------------------------------------------------

class RTHandler(http.server.BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path.startswith("/vehicle_positions.pb"):
            with _pb_lock:
                data = _latest_pb

            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(data)

        elif self.path == "/":
            with _pb_lock:
                size = len(_latest_pb)

            feed = gtfs_realtime_pb2.FeedMessage()
            if size:
                feed.ParseFromString(_latest_pb)
            n = len(feed.entity)

            trip_map = get_trip_id_map()
            body = (
                f'{{"vehicles": {n}, '
                f'"trip_map_entries": {len(trip_map)}, '
                f'"pb_size_bytes": {size}, '
                f'"updated": "{_ts()}"}}'
            ).encode()

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):
        pass


def start_http_server(port: int):
    server = http.server.HTTPServer(("", port), RTHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"    http://localhost:{port}/vehicle_positions.pb  ← protobuf")
    print(f"    http://localhost:{port}/                      ← status JSON")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("=== MZK Pabianice – GTFS-Realtime VehiclePosition ===\n")

    print("[1] Ladowanie mapy trip_id z trips.txt...")
    get_trip_id_map()  # wstępne załadowanie + log

    print("\n[2] Pierwsze pobranie pozycji...")
    try:
        vehicles = fetch_all_vehicles()
        pb_bytes = build_feed(vehicles)
        with _pb_lock:
            global _latest_pb
            _latest_pb = pb_bytes
        out_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
        with open(out_path, "wb") as f:
            f.write(pb_bytes)
        print(f"  ok {len(vehicles)} pojazdów, {len(pb_bytes)} B")
    except Exception as e:
        print(f"  [ERR] {e}")

    print("\n[3] Start serwera HTTP...")
    start_http_server(HTTP_PORT)

    print("\n[4] Start petli pollingu (co 30s)...")
    poll_thread = threading.Thread(target=polling_loop, daemon=True)
    poll_thread.start()

    print("\n  Nacisnij Ctrl+C aby zatrzymac.\n")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nZatrzymano.")


if __name__ == "__main__":
    main()