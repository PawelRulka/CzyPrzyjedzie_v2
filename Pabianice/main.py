"""
MZK Pabianice – scraper GTFS (async)
======================================
Generuje pliki GTFS na podstawie danych ze strony komunikacjapabianice.pl

Wymagania:
    pip install aiohttp beautifulsoup4

Uruchomienie:
    python mzk_pabianice_gtfs_scraper.py

Wygenerowane pliki trafią do katalogu ./gtfs_output/
Po zakończeniu skrypt spakuje je do gtfs_output/pabianice.zip
i uruchomi serwer HTTP na http://localhost:8090/pabianice.zip
"""

import asyncio
import csv
import http.server
import os
import re
import subprocess
import sys
import threading
import time
import zipfile
from datetime import date, datetime, timedelta

import aiohttp
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Konfiguracja
# ---------------------------------------------------------------------------

BASE_URL      = "https://komunikacjapabianice.pl/rj"
OUTPUT_DIR    = "gtfs_output"
TID           = 0
DAYS_AHEAD    = 90
HTTP_PORT     = 8090
# Kolory linii: route_short_name -> (route_color, route_text_color)
ROUTE_COLORS: dict = {
    "1":   ("015D92", "FFFFFF"),
    "2":   ("015D92", "FFFFFF"),
    "3":   ("015D92", "FFFFFF"),
    "4":   ("015D92", "FFFFFF"),
    "5":   ("015D92", "FFFFFF"),
    "6":   ("015D92", "FFFFFF"),
    "7":   ("015D92", "FFFFFF"),
    "A41": ("015D92", "FFFFFF"),
    "260": ("FDAE02", "FFFFFF"),
    "261": ("FDAE02", "FFFFFF"),
    "262": ("FDAE02", "FFFFFF"),
    "263": ("FDAE02", "FFFFFF"),
    "265": ("FDAE02", "FFFFFF"),
    "T":   ("FDAE02", "FFFFFF"),
}

# Ile żądań HTTP jednocześnie (semaphore).
# Przy zbyt wysokiej wartości serwer może zwracać 429/503.
CONCURRENCY   = 100
# Krótkie opóźnienie między żądaniami w jednym slocie (grzeczność)
REQUEST_DELAY = 0.05
MAX_RETRIES   = 3

NOW      = datetime.now()
TODAY    = date.today()
END_DATE = TODAY + timedelta(days=DAYS_AHEAD)

HEADERS = {
    "User-Agent": "jp ideologie firma reprezentuje jp rozprzestrzenia sie firma wciaz trzyma klase jp czy ty znasz ten skrot jp kazdy pies to fiut jp z tradycjami klub jp kopie kurwom grob"
}


# ---------------------------------------------------------------------------
# Async HTTP client
# ---------------------------------------------------------------------------

class APIClient:
    def __init__(self, session: aiohttp.ClientSession, sem: asyncio.Semaphore):
        self.session = session
        self.sem     = sem

    async def get_text(self, url: str) -> str | None:
        """Pobiera URL i zwraca tekst odpowiedzi."""
        async with self.sem:
            await asyncio.sleep(REQUEST_DELAY)
            for attempt in range(MAX_RETRIES):
                try:
                    async with self.session.get(
                        url,
                        timeout=aiohttp.ClientTimeout(total=30),
                        ssl=False,
                    ) as resp:
                        resp.raise_for_status()
                        return await resp.text()
                except Exception as e:
                    if attempt == MAX_RETRIES - 1:
                        print(f"  [WARN] {url} – nieudane po {MAX_RETRIES} probach: {e}")
                        return None
                    await asyncio.sleep(2 ** attempt)
        return None

    async def get_json(self, url: str):
        """Pobiera URL i parsuje JSON."""
        text = await self.get_text(url)
        if text is None:
            return None
        import json
        try:
            return json.loads(text)
        except Exception as e:
            print(f"  [WARN] JSON parse error dla {url}: {e}")
            return None


# ---------------------------------------------------------------------------
# Pomocnicze
# ---------------------------------------------------------------------------

def soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def write_csv(filename: str, fieldnames: list, rows: list):
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  ok {filename}  ({len(rows)} wierszy)")


# ---------------------------------------------------------------------------
# 1. agency.txt
# ---------------------------------------------------------------------------

def build_agency():
    rows = [{
        "agency_id":       "MZK",
        "agency_name":     "Miejski Zakład Komunikacyjny Sp. z o.o. w Pabianicach",
        "agency_url":      "http://www.mzkpabianice.pl",
        "agency_timezone": "Europe/Warsaw",
        "agency_lang":     "pl",
        "agency_phone":    "+48422155195",
    }]
    write_csv("agency.txt", list(rows[0].keys()), rows)


# ---------------------------------------------------------------------------
# 2. stops.txt
# ---------------------------------------------------------------------------

async def build_stops(api: APIClient):
    url  = f"{BASE_URL}/BusStops/GetMapBusStopListJson?q=&tId={TID}"
    data = await api.get_json(url)
    if not data:
        raise RuntimeError("Nie udalo sie pobrac przystankow")

    rows = []
    for entry in data:
        rows.append({
            "stop_id":   entry[0],
            "stop_name": entry[1],
            "stop_lat":  entry[5],
            "stop_lon":  entry[4],
            "zone_id":   entry[7],
        })

    write_csv("stops.txt",
              ["stop_id", "stop_name", "stop_lat", "stop_lon", "zone_id"],
              rows)
    return {r["stop_id"]: r for r in rows}


# ---------------------------------------------------------------------------
# 3. routes.txt
# ---------------------------------------------------------------------------

ROUTE_TYPE_BUS = 3

async def build_routes(api: APIClient) -> dict:
    html = await api.get_text(f"{BASE_URL}?tId={TID}")
    if not html:
        raise RuntimeError("Nie udalo sie pobrac listy linii")
    s = soup(html)

    routes = {}
    for section in s.select("div.section"):
        box = section.select_one("div.route-box-variant-2")
        if not box:
            continue
        short_name = box.get_text(strip=True)

        btn = section.select_one("button.direction-from-to[onclick]")
        if not btn:
            continue
        m = re.search(r"/Routes/Track/(\d+)", btn["onclick"])
        if not m:
            continue
        route_id = int(m.group(1))
        routes[route_id] = short_name

    rows = []
    for route_id, short_name in sorted(routes.items(), key=lambda x: x[1]):
        rows.append({
            "route_id":         route_id,
            "agency_id":        "MZK",
            "route_short_name": short_name,
            "route_long_name":  "",
            "route_type":       ROUTE_TYPE_BUS,
            "route_color":      ROUTE_COLORS.get(short_name, ("", ""))[0],
            "route_text_color": ROUTE_COLORS.get(short_name, ("", ""))[1],
        })

    write_csv("routes.txt",
              ["route_id", "agency_id", "route_short_name",
               "route_long_name", "route_type", "route_color", "route_text_color"],
              rows)
    return routes


# ---------------------------------------------------------------------------
# 4. calendar.txt + calendar_dates.txt
# ---------------------------------------------------------------------------

def build_calendar_and_dates(date_service_map: dict):
    write_csv("calendar.txt",
              ["service_id", "monday", "tuesday", "wednesday",
               "thursday", "friday", "saturday", "sunday",
               "start_date", "end_date"],
              [])

    cal_date_rows = [
        {
            "service_id":     sid,
            "date":           d.strftime("%Y%m%d"),
            "exception_type": 1,
        }
        for d, sid in sorted(date_service_map.items())
    ]
    write_csv("calendar_dates.txt",
              ["service_id", "date", "exception_type"],
              cal_date_rows)


# ---------------------------------------------------------------------------
# 5. GetTracks
# ---------------------------------------------------------------------------

async def fetch_tracks(api: APIClient, route_id: int) -> dict:
    url = f"{BASE_URL}/Routes/GetTracks?routeId={route_id}&tId={TID}&transits=1"
    raw = await api.get_json(url)
    if not raw:
        return {"stops": [], "segments": [], "directions": []}

    global_stops = [
        {"stop_id": s[0], "stop_name": s[1], "lon": s[3], "lat": s[4]}
        for s in raw[0]
    ]

    segments = []
    for seg in raw[2]:
        coords_flat = seg[3]
        coords = [(coords_flat[i], coords_flat[i+1])
                  for i in range(0, len(coords_flat) - 1, 2)]
        segments.append({"from_idx": seg[1], "to_idx": seg[2], "coords": coords})

    directions = []
    for d in raw[3]:
        stop_idx_arr = d[6][0] if d[6] and d[6][0] else []
        directions.append({
            "dir_id":       d[0],
            "dir_sub_id":   d[2],
            "direction":    d[1],
            "from":         d[3],
            "to":           d[4],
            "headsign":     d[4],
            "stop_indices": stop_idx_arr,
        })

    return {"stops": global_stops, "segments": segments, "directions": directions}


# ---------------------------------------------------------------------------
# 6. shapes.txt
# ---------------------------------------------------------------------------

async def build_shapes(api: APIClient, routes: dict) -> dict:
    shape_rows = []
    shape_map  = {}

    # Pobieramy GetTracks dla wszystkich linii równolegle
    track_tasks = {
        route_id: fetch_tracks(api, route_id)
        for route_id in routes
    }
    results = await asyncio.gather(*track_tasks.values(), return_exceptions=True)
    track_data_map = {}
    for route_id, result in zip(track_tasks.keys(), results):
        if isinstance(result, Exception):
            print(f"  [ERR] GetTracks linii {routes[route_id]}: {result}")
        else:
            track_data_map[route_id] = result

    for route_id, data in track_data_map.items():
        for d in data["directions"]:
            dir_id       = d["dir_id"]
            stop_indices = d["stop_indices"]
            if not stop_indices:
                continue

            shape_id = f"SHP_{route_id}_{dir_id}"
            shape_map[(route_id, dir_id)] = shape_id
            pt_seq = 1

            for i in range(len(stop_indices)):
                idx = stop_indices[i]
                if idx >= len(data["stops"]):
                    continue
                stop = data["stops"][idx]
                shape_rows.append({
                    "shape_id":            shape_id,
                    "shape_pt_lat":        stop["lat"],
                    "shape_pt_lon":        stop["lon"],
                    "shape_pt_sequence":   pt_seq,
                    "shape_dist_traveled": "",
                })
                pt_seq += 1

                if i + 1 < len(stop_indices):
                    next_idx = stop_indices[i + 1]
                    for seg in data["segments"]:
                        if seg["from_idx"] == idx and seg["to_idx"] == next_idx:
                            for (lon, lat) in seg["coords"]:
                                shape_rows.append({
                                    "shape_id":            shape_id,
                                    "shape_pt_lat":        lat,
                                    "shape_pt_lon":        lon,
                                    "shape_pt_sequence":   pt_seq,
                                    "shape_dist_traveled": "",
                                })
                                pt_seq += 1
                            break

    write_csv("shapes.txt",
              ["shape_id", "shape_pt_lat", "shape_pt_lon",
               "shape_pt_sequence", "shape_dist_traveled"],
              shape_rows)
    return shape_map, track_data_map


# ---------------------------------------------------------------------------
# 7. Parsowanie dziennego Timetable HTML
# ---------------------------------------------------------------------------

def parse_timetable_html_daily(html: str) -> list:
    s       = soup(html)
    results = []

    table = s.select_one("table.tt-hidden-table")
    if not table:
        return results

    for row in table.select("tr[hour-value]"):
        hour = int(row["hour-value"])
        for span in row.select("span.departure"):
            cls     = " ".join(span.get("class", []))
            m       = re.search(r"(\d+)_(\d+)", cls)
            dir_key = m.group(0) if m else "unknown"

            onclick = span.get("onclick", "")
            tm      = re.search(r"/Trip/Index/(\d+)", onclick)
            trip_id = int(tm.group(1)) if tm else None

            minute_span = span.select_one("span.minute")
            if not minute_span:
                continue

            aria = minute_span.get("aria-label", "")
            am   = re.match(r"(\d{1,2}):(\d{2})", aria)
            if am:
                h2, mn = int(am.group(1)), int(am.group(2))
            else:
                h2 = hour
                mn = int(minute_span.get_text(strip=True))

            results.append((dir_key, trip_id, f"{h2:02d}:{mn:02d}:00"))

    return results


# ---------------------------------------------------------------------------
# 8. Parsowanie Trip/Index -> stop_times
# ---------------------------------------------------------------------------

def parse_trip_stop_times(html: str) -> list:
    s   = soup(html)
    svg = s.select_one("div.graph svg")
    if not svg:
        return []

    results      = []
    pending_time = None
    pending_nz   = False

    for t in svg.find_all("text"):
        cls = " ".join(t.get("class", []))

        if "graph-bs-text-time" in cls:
            raw = t.get_text(strip=True)
            m   = re.match(r"(\d{1,2}):(\d{2})", raw)
            if m:
                pending_time = f"{int(m.group(1)):02d}:{m.group(2)}:00"
            pending_nz = "nz" in cls

        elif "graph-bs-text" in cls and "graph-bs-text-time" not in cls:
            bsi = t.get("bsi")
            if bsi and pending_time:
                results.append({
                    "stop_id": int(bsi),
                    "time":    pending_time,
                    "is_nz":   pending_nz,
                })
                pending_time = None
                pending_nz   = False

    return results


# ---------------------------------------------------------------------------
# 9. Główna pętla async: trips.txt + stop_times.txt
# ---------------------------------------------------------------------------

async def build_trips_and_stop_times(
    api: APIClient,
    routes: dict,
    shape_map: dict,
    track_data_map: dict,
) -> dict:
    trip_rows        = []
    stop_time_rows   = []
    trip_ids_seen    = set()
    date_service_map = {}

    date_range = [TODAY + timedelta(days=i) for i in range(DAYS_AHEAD + 1)]

    # ── Krok A: zbierz wszystkie URLe Timetable i pobierz je równolegle ────

    # Budujemy listę zadań: (route_id, direction_info, current_date)
    TimetableTask = tuple  # (route_id, direction, current_date, url)
    tt_tasks: list[TimetableTask] = []

    for route_id, short_name in routes.items():
        data = track_data_map.get(route_id)
        if not data:
            continue

        for direction in data["directions"]:
            stop_indices = direction["stop_indices"]
            if not stop_indices:
                continue
            first_idx = stop_indices[0]
            if first_idx >= len(data["stops"]):
                continue
            first_stop    = data["stops"][first_idx]
            start_stop_id = first_stop["stop_id"]
            dir_num       = direction["direction"]

            for current_date in date_range:
                date_str = current_date.strftime("%Y-%m-%d")
                url = (
                    f"{BASE_URL}/Timetable"
                    f"?stopId={start_stop_id}"
                    f"&routeId={route_id}"
                    f"&dir={dir_num}"
                    f"&tId={TID}"
                    f"&date={date_str}"
                    f"&start={date_str}"
                )
                tt_tasks.append((route_id, direction, current_date, start_stop_id, url))

    total_tt = len(tt_tasks)
    print(f"    Timetable: {total_tt} zapytan do wykonania ({CONCURRENCY} równoległych)...")

    # Pobieramy wszystkie Timetable równolegle
    tt_htmls = await asyncio.gather(
        *[api.get_text(task[4]) for task in tt_tasks],
        return_exceptions=True,
    )

    # ── Krok B: parsuj wyniki Timetable, zbierz unikalne trip_id ───────────

    # trip_id -> (route_id, direction, service_id, start_stop_id, dep_time)
    trips_to_fetch: dict[int, tuple] = {}

    for (route_id, direction, current_date, start_stop_id, url), html in zip(tt_tasks, tt_htmls):
        if isinstance(html, Exception) or html is None:
            continue

        dir_id     = direction["dir_id"]
        service_id = f"DATE_{current_date.strftime('%Y%m%d')}"
        gtfs_dir   = max(0, direction["direction"] - 1)
        shape_id   = shape_map.get((route_id, dir_id), "")
        headsign   = direction["headsign"]

        departures = parse_timetable_html_daily(html)
        matching   = [
            (dk, tid, dt) for (dk, tid, dt) in departures
            if dk.startswith(str(dir_id)) and tid is not None
        ]

        if matching:
            date_service_map[current_date] = service_id

        for (dir_key, trip_id, dep_time) in matching:
            if trip_id in trip_ids_seen:
                continue
            trip_ids_seen.add(trip_id)

            trip_rows.append({
                "route_id":      route_id,
                "service_id":    service_id,
                "trip_id":       trip_id,
                "trip_headsign": headsign,
                "direction_id":  gtfs_dir,
                "shape_id":      shape_id,
            })
            trips_to_fetch[trip_id] = (start_stop_id, dep_time)

    print(f"    Znaleziono {len(trip_rows)} kursow, {len(trips_to_fetch)} unikalnych trip_id")
    print(f"    Stop_times: {len(trips_to_fetch)} zapytan do Trip/Index...")

    # ── Krok C: pobierz stop_times dla wszystkich unikalnych kursów równolegle

    trip_id_list = list(trips_to_fetch.keys())
    trip_urls    = [
        f"{BASE_URL}/Trip/Index/{tid}?busStopId={trips_to_fetch[tid][0]}&tId={TID}"
        for tid in trip_id_list
    ]

    trip_htmls = await asyncio.gather(
        *[api.get_text(url) for url in trip_urls],
        return_exceptions=True,
    )

    for trip_id, html in zip(trip_id_list, trip_htmls):
        start_stop_id, dep_time = trips_to_fetch[trip_id]

        if isinstance(html, Exception) or html is None:
            st_list = [{"stop_id": start_stop_id, "time": dep_time, "is_nz": False}]
        else:
            st_list = parse_trip_stop_times(html)
            if not st_list:
                st_list = [{"stop_id": start_stop_id, "time": dep_time, "is_nz": False}]

        for seq, st in enumerate(st_list, start=1):
            pt = 2 if st["is_nz"] else 0
            stop_time_rows.append({
                "trip_id":        trip_id,
                "arrival_time":   st["time"],
                "departure_time": st["time"],
                "stop_id":        st["stop_id"],
                "stop_sequence":  seq,
                "pickup_type":    pt,
                "drop_off_type":  pt,
            })

    write_csv("trips.txt",
              ["route_id", "service_id", "trip_id",
               "trip_headsign", "direction_id", "shape_id"],
              trip_rows)

    write_csv("stop_times.txt",
              ["trip_id", "arrival_time", "departure_time",
               "stop_id", "stop_sequence", "pickup_type", "drop_off_type"],
              stop_time_rows)

    return date_service_map


# ---------------------------------------------------------------------------
# 10. feed_info.txt
# ---------------------------------------------------------------------------

def build_feed_info():
    rows = [{
        "feed_lang":           "pl",
        "feed_version":        NOW.strftime("%Y%m%d%H%M%S"),
        "feed_start_date":     TODAY.strftime("%Y%m%d"),
        "feed_end_date":       END_DATE.strftime("%Y%m%d"),
        "feed_publisher_url":  "https://github.com/pawlinski07/pawlinski-gtfs",
        "feed_contact_url":    "https://github.com/pawlinski07/pawlinski-gtfs/issues",
        "feed_publisher_name": "Unofficial GTFS of MZK Pabianice, generated by pawlinski07",
    }]
    write_csv("feed_info.txt", list(rows[0].keys()), rows)


# ---------------------------------------------------------------------------
# 11. Pakowanie do ZIP
# ---------------------------------------------------------------------------

GTFS_FILES = [
    "agency.txt", "stops.txt", "routes.txt",
    "calendar.txt", "calendar_dates.txt", "shapes.txt",
    "trips.txt", "stop_times.txt", "feed_info.txt",
]

def build_zip() -> str:
    zip_path = os.path.join(OUTPUT_DIR, "pabianice.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname in GTFS_FILES:
            fpath = os.path.join(OUTPUT_DIR, fname)
            if os.path.exists(fpath):
                zf.write(fpath, arcname=fname)
                print(f"  + {fname}")
            else:
                print(f"  [SKIP] {fname} nie istnieje")
    size_kb = os.path.getsize(zip_path) // 1024
    print(f"  ok pabianice.zip  ({size_kb} KB)")
    return zip_path


# ---------------------------------------------------------------------------
# 12. Async main
# ---------------------------------------------------------------------------

async def async_main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print("=== MZK Pabianice – generator GTFS (async) ===")
    print(f"    Zakres dat: {TODAY} -> {END_DATE} ({DAYS_AHEAD} dni)")
    print(f"    Wspolbieznosc: {CONCURRENCY} równoległych zapytan\n")

    sem       = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, limit_per_host=CONCURRENCY, ssl=False)

    async with aiohttp.ClientSession(
        connector=connector,
        headers=HEADERS,
    ) as session:
        api = APIClient(session, sem)

        print("[1/8] agency.txt")
        build_agency()

        print("\n[2/8] stops.txt")
        await build_stops(api)

        print("\n[3/8] routes.txt")
        routes = await build_routes(api)
        print(f"  Znaleziono {len(routes)} linii: {', '.join(sorted(routes.values()))}")

        print("\n[4/8] shapes.txt  (GetTracks rownolegly)")
        shape_map, track_data_map = await build_shapes(api, routes)

        print("\n[5/8] trips.txt + stop_times.txt  (Timetable + Trip/Index rownolegly)")
        date_service_map = await build_trips_and_stop_times(
            api, routes, shape_map, track_data_map
        )

    print("\n[6/8] calendar.txt + calendar_dates.txt")
    build_calendar_and_dates(date_service_map)

    print("\n[7/8] feed_info.txt")
    build_feed_info()

    print("\n[8/8] Pakowanie ZIP...")
    build_zip()

    print(f"\nGotowe! Pliki GTFS zapisane w: {OUTPUT_DIR}/")
    print("\nWalidacja (opcjonalna):")
    print("  pip install gtfs-kit")
    print(f"  python -c \"import gtfs_kit; f=gtfs_kit.read_feed('{OUTPUT_DIR}/', '1'); print(f.validate())\"")


# ---------------------------------------------------------------------------
# 13. Serwer HTTP + uruchomienie RT
# ---------------------------------------------------------------------------

def start_servers():
    abs_dir = os.path.abspath(OUTPUT_DIR)

    class _Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=abs_dir, **kwargs)
        def log_message(self, fmt, *args):
            pass

    static_server = http.server.HTTPServer(("", HTTP_PORT), _Handler)
    t = threading.Thread(target=static_server.serve_forever, daemon=True)
    t.start()
    print(f"\nSerwer HTTP (Static): http://localhost:{HTTP_PORT}/pabianice.zip")

    rt_script = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             "mzk_pabianice_gtfs_rt.py")
    if os.path.exists(rt_script):
        subprocess.Popen([sys.executable, rt_script])
        print("Serwer HTTP (RT):     http://localhost:8091/vehicle_positions.pb")
    else:
        print(f"[INFO] {rt_script} nie znaleziony – RT nie uruchomiony.")

    print("\nNacisnij Ctrl+C aby zatrzymac.\n")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        static_server.shutdown()
        print("\nZatrzymano.")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    t0 = time.time()
    asyncio.run(async_main())
    elapsed = time.time() - t0
    print(f"\nCzas generacji: {elapsed:.0f}s ({elapsed/60:.1f} min)")
    start_servers()


if __name__ == "__main__":
    main()