# CzyPrzyjedzieApp/api.py
from django.http import JsonResponse
from django.views.decorators.http import require_GET
from django.shortcuts import get_object_or_404

from datetime import datetime, timedelta, date as date_type
import math
import requests
from google.transit import gtfs_realtime_pb2

from .models import City, GTFSFeed
from . import gtfs_loader


# ---------------------------------------------------------------------------
# Helpers — czas i geometria
# ---------------------------------------------------------------------------

def error(msg, status=400):
    return JsonResponse({"status": "error", "message": msg}, status=status)


def parse_date_from_time(date: datetime, time_str: str):
    """Zwraca datetime uwzględniający godziny >24:00"""
    h, m, s = map(int, time_str.split(":"))
    days_offset = h // 24
    h = h % 24
    return (date + timedelta(days=days_offset)).replace(hour=h, minute=m, second=s)


def time_to_seconds(time_str: str) -> int | None:
    """Konwertuje 'HH:MM:SS' (w tym >24h) na sekundy od początku doby. Zwraca None przy błędzie."""
    if not time_str:
        return None
    try:
        h, m, s = map(int, time_str.split(":"))
        return h * 3600 + m * 60 + s
    except Exception:
        return None


def seconds_to_time(secs: int) -> str:
    """Konwertuje sekundy od początku doby na 'HH:MM:SS' (może być >24h)."""
    secs = int(secs)
    h = secs // 3600
    m = (secs % 3600) // 60
    s = secs % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Odległość w kilometrach między dwoma punktami geograficznymi."""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def _current_time_seconds() -> int:
    """Aktualna godzina jako sekundy od początku doby (lokalny czas serwera)."""
    now = datetime.now()
    return now.hour * 3600 + now.minute * 60 + now.second


# ---------------------------------------------------------------------------
# Realtime — pobieranie i parsowanie feedów
# ---------------------------------------------------------------------------

def load_realtime(feed: GTFSFeed) -> dict:
    """
    Pobiera dane realtime dla feedu.
    Zwraca dict: {"vehicle_positions": ..., "trip_updates": ..., "alerts": ...}
    Każde pole to FeedMessage (protobuf), dict/lista (JSON) lub None.
    """
    data = {"vehicle_positions": None, "trip_updates": None, "alerts": None}

    def load_url(url):
        if not url:
            return None
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            content_type = resp.headers.get("Content-Type", "")
            if url.endswith(".json") or "application/json" in content_type:
                return resp.json()
            feed_pb = gtfs_realtime_pb2.FeedMessage()
            feed_pb.ParseFromString(resp.content)
            return feed_pb
        except Exception:
            return None

    data["vehicle_positions"] = load_url(feed.vehicle_positions_url)
    data["trip_updates"] = load_url(feed.trip_updates_url)
    data["alerts"] = load_url(feed.service_alerts_url)
    return data


def extract_vehicle_for_trip(realtime: dict, trip_id: str) -> dict:
    """Zwraca dane pojazdu (vehicle_number, lat, lon) pasującego do trip_id."""
    vp = realtime.get("vehicle_positions")
    if vp is None:
        return {}

    if isinstance(vp, gtfs_realtime_pb2.FeedMessage):
        for entity in vp.entity:
            if entity.HasField("vehicle"):
                v = entity.vehicle
                if v.trip.trip_id == trip_id:
                    return {
                        "vehicle_number": entity.id,
                        "lat": v.position.latitude,
                        "lon": v.position.longitude,
                    }
        return {}

    entities = vp if isinstance(vp, list) else vp.get("entity", [])
    for entity in entities:
        vehicle = entity.get("vehicle", {})
        if vehicle.get("trip", {}).get("trip_id") == trip_id:
            position = vehicle.get("position", {})
            vehicle_info = vehicle.get("vehicle", {})
            return {
                "vehicle_number": vehicle_info.get("id") or entity.get("id"),
                "lat": position.get("latitude"),
                "lon": position.get("longitude"),
            }
    return {}


def extract_trip_updates_for_trip(realtime: dict, trip_id: str) -> list:
    """Zwraca listę stop_time_update z TripUpdates dla danego trip_id."""
    tu = realtime.get("trip_updates")
    if tu is None:
        return []

    if isinstance(tu, gtfs_realtime_pb2.FeedMessage):
        for entity in tu.entity:
            if entity.HasField("trip_update"):
                upd = entity.trip_update
                if upd.trip.trip_id == trip_id:
                    result = []
                    for stu in upd.stop_time_update:
                        result.append({
                            "stop_id": stu.stop_id,
                            "stop_sequence": stu.stop_sequence,
                            "arrival_delay": stu.arrival.delay if stu.HasField("arrival") else None,
                            "departure_delay": stu.departure.delay if stu.HasField("departure") else None,
                            "arrival_time": stu.arrival.time if stu.HasField("arrival") else None,
                            "departure_time": stu.departure.time if stu.HasField("departure") else None,
                        })
                    return result
        return []

    entities = tu if isinstance(tu, list) else tu.get("entity", [])
    for entity in entities:
        trip_update = entity.get("trip_update", {})
        if trip_update.get("trip", {}).get("trip_id") == trip_id:
            result = []
            for stu in trip_update.get("stop_time_update", []):
                result.append({
                    "stop_id": stu.get("stop_id"),
                    "stop_sequence": stu.get("stop_sequence"),
                    "arrival_delay": stu.get("arrival", {}).get("delay"),
                    "departure_delay": stu.get("departure", {}).get("delay"),
                    "arrival_time": stu.get("arrival", {}).get("time"),
                    "departure_time": stu.get("departure", {}).get("time"),
                })
            return result
    return []


# ---------------------------------------------------------------------------
# Niskopozimowe helpery estymacji
# ---------------------------------------------------------------------------

def _find_nearest_stop(
    stops: list,
    vehicle_lat: float,
    vehicle_lon: float,
    stops_by_id: dict,
) -> int | None:
    """
    Zwraca indeks (w posortowanej liście stops) przystanku najbliższego
    aktualnej pozycji pojazdu. Zwraca None gdy brak współrzędnych.
    """
    nearest_idx = None
    nearest_dist = float("inf")
    for i, st in enumerate(stops):
        info = stops_by_id.get(str(st.get("stop_id", "")))
        if not info:
            continue
        try:
            d = haversine(vehicle_lat, vehicle_lon,
                          float(info["stop_lat"]), float(info["stop_lon"]))
        except (KeyError, TypeError, ValueError):
            continue
        if d < nearest_dist:
            nearest_dist = d
            nearest_idx = i
    return nearest_idx


def _delay_from_position(
    stops: list,
    nearest_idx: int,
    now_s: int,
) -> int:
    """
    Oblicza opóźnienie w sekundach na podstawie indeksu przystanku
    najbliższego pojazdowi i aktualnej godziny.
    Wynik może być ujemny (autobus jedzie przed czasem).
    """
    sched_dep = stops[nearest_idx].get("departure_time", "")
    sched_dep_s = time_to_seconds(sched_dep)
    if sched_dep_s is None:
        return 0
    return now_s - sched_dep_s


def _build_tu_lookups(trip_updates: list) -> tuple[dict, dict]:
    """Buduje słowniki TripUpdates po stop_sequence i stop_id."""
    by_seq = {}
    by_sid = {}
    for u in trip_updates:
        if u.get("stop_sequence") is not None:
            by_seq[int(u["stop_sequence"])] = u
        if u.get("stop_id") is not None:
            by_sid[str(u["stop_id"])] = u
    return by_seq, by_sid


def _last_known_tu_delay(trip_updates: list) -> int | None:
    """
    Zwraca opóźnienie z ostatniego przystanku w TripUpdates który ma dane
    o delay. Używane do propagacji gdy brakuje danych dla danego przystanku.
    """
    best_seq = -1
    best_delay = None
    for u in trip_updates:
        delay = u.get("departure_delay") if u.get("departure_delay") is not None else u.get("arrival_delay")
        if delay is None:
            continue
        seq = int(u["stop_sequence"]) if u.get("stop_sequence") is not None else -1
        if seq > best_seq:
            best_seq = seq
            best_delay = delay
    return best_delay


def _resolve_delay_for_stop(
    seq: int,
    sid: str,
    tu_by_seq: dict,
    tu_by_sid: dict,
    fallback_delay: int | None,
    source_if_tu: str = "trip_update",
    source_if_fallback: str = "estimated",
) -> tuple[int, str]:
    """
    Wyznacza (delay_seconds, source) dla konkretnego przystanku.

    Hierarchia:
      1. TripUpdates — jeśli zawiera delay dla tego przystanku
      2. fallback_delay — z pozycji pojazdu lub ostatniego TripUpdates z delay
      3. 0 / "static" — gdy brak jakichkolwiek danych
    """
    update = tu_by_seq.get(seq) or tu_by_sid.get(sid)
    if update is not None:
        dep = update.get("departure_delay")
        arr = update.get("arrival_delay")
        delay = dep if dep is not None else arr
        if delay is not None:
            return delay, source_if_tu

    # TripUpdates nie miał danych dla tego przystanku (lub był niekompletny)
    if fallback_delay is not None:
        return fallback_delay, source_if_fallback

    return 0, "static"


# ---------------------------------------------------------------------------
# Główna logika: budowanie wzbogaconych stop_times
# ---------------------------------------------------------------------------

def build_stop_times_with_realtime(
    static_stops: list,
    trip_updates: list,
    vehicle_lat: float | None,
    vehicle_lon: float | None,
    stops_by_id: dict,
) -> list:
    """
    Buduje pełną listę przystanków kursu wzbogaconą o dane realtime.

    Każdy element zawiera:
        stop_id, stop_sequence
        scheduled_arrival, scheduled_departure   ← zawsze ze statiku
        real_arrival, real_departure             ← RT lub estymacja
        delay_seconds                            ← może być ujemne (przed czasem)
        status: 'passed' | 'current' | 'upcoming'
        source: 'trip_update' | 'estimated' | 'static'

    Logika priorytetu delay dla przystanku:
      1. TripUpdates ma delay dla tego przystanku → używamy go
      2. TripUpdates jest niekompletny (brak delay dla tego stopu) →
           a. pozycja pojazdu → estymacja haversine
           b. ostatni znany delay z TripUpdates → propagujemy
      3. Brak TripUpdates, jest pozycja → haversine
      4. Brak czegokolwiek → delay=0, source='static'

    Reguły dotyczące przystanków 'passed':
      - Jeśli status='passed' i source='trip_update' → zachowujemy dane RT
      - Jeśli status='passed' i source='estimated'   → NIE aktualizujemy;
        real_* = scheduled, delay = None (brak danych wstecznych)

    Ujemne delay (przed czasem) jest w pełni obsługiwane.
    """
    if not static_stops:
        return []

    stops = sorted(static_stops, key=lambda x: int(x.get("stop_sequence", 0)))
    now_s = _current_time_seconds()

    has_position = vehicle_lat is not None and vehicle_lon is not None
    has_tu = bool(trip_updates)

    # Lookups TripUpdates
    tu_by_seq, tu_by_sid = _build_tu_lookups(trip_updates) if has_tu else ({}, {})

    # Ostatni znany delay z TripUpdates (do propagacji gdy TU niekompletny)
    last_tu_delay = _last_known_tu_delay(trip_updates) if has_tu else None

    # Pozycja → indeks najbliższego przystanku i delay z pozycji
    nearest_idx = None
    pos_delay = None
    if has_position:
        nearest_idx = _find_nearest_stop(stops, vehicle_lat, vehicle_lon, stops_by_id)
        if nearest_idx is not None:
            pos_delay = _delay_from_position(stops, nearest_idx, now_s)

    # Fallback delay — priorytet: pozycja > ostatni TU > None
    fallback_delay = pos_delay if pos_delay is not None else last_tu_delay
    fallback_source = "estimated" if pos_delay is not None else (
        "estimated" if last_tu_delay is not None else "static"
    )

    result = []
    for i, st in enumerate(stops):
        seq = int(st.get("stop_sequence", 0))
        sid = str(st.get("stop_id", ""))
        sched_arr = st.get("arrival_time", "")
        sched_dep = st.get("departure_time", "")
        sched_arr_s = time_to_seconds(sched_arr)
        sched_dep_s = time_to_seconds(sched_dep)

        # Wyznacz status przystanku
        # Używamy nearest_idx jeśli jest dostępny (dokładniejsze niż czas)
        if nearest_idx is not None:
            if i < nearest_idx:
                status = "passed"
            elif i == nearest_idx:
                status = "current"
            else:
                status = "upcoming"
        else:
            # Brak pozycji — status na podstawie godziny (bufor 60 s)
            if sched_dep_s is not None and now_s > sched_dep_s + 60:
                status = "passed"
            else:
                status = "upcoming"

        # Wyznacz delay i source
        delay, source = _resolve_delay_for_stop(
            seq, sid, tu_by_seq, tu_by_sid,
            fallback_delay=fallback_delay,
            source_if_fallback=fallback_source,
        )

        # Reguła: przystanki 'passed' z estymacją → NIE aktualizujemy delay
        if status == "passed" and source != "trip_update":
            result.append({
                "stop_id": sid,
                "stop_sequence": seq,
                "scheduled_arrival": sched_arr,
                "scheduled_departure": sched_dep,
                "real_arrival": None,
                "real_departure": None,
                "delay_seconds": None,   # brak danych wstecznych
                "status": status,
                "source": "static",
            })
            continue

        # Oblicz real_* (ujemny delay = przed czasem — poprawne zachowanie)
        real_arr = seconds_to_time(sched_arr_s + delay) if sched_arr_s is not None else None
        real_dep = seconds_to_time(sched_dep_s + delay) if sched_dep_s is not None else None

        result.append({
            "stop_id": sid,
            "stop_sequence": seq,
            "scheduled_arrival": sched_arr,
            "scheduled_departure": sched_dep,
            "real_arrival": real_arr,
            "real_departure": real_dep,
            "delay_seconds": delay,
            "status": status,
            "source": source,
        })

    return result


def get_single_stop_realtime(
    trip_id: str,
    stop_id: str,
    stop_sequence: int,
    sched_arr: str,
    sched_dep: str,
    realtime: dict,
    all_trip_stops: list,
    stops_by_id: dict,
) -> dict:
    """
    Oblicza status/delay/real_* dla jednego przystanku jednego kursu.
    Używane przez get_schedule_for_stop.

    Zwraca dict z: status, real_arrival, real_departure, delay_seconds, source.
    """
    now_s = _current_time_seconds()
    sched_arr_s = time_to_seconds(sched_arr)
    sched_dep_s = time_to_seconds(sched_dep)

    vehicle_data = extract_vehicle_for_trip(realtime, trip_id)
    trip_updates = extract_trip_updates_for_trip(realtime, trip_id)

    vehicle_lat = vehicle_data.get("lat")
    vehicle_lon = vehicle_data.get("lon")
    has_position = vehicle_lat is not None and vehicle_lon is not None
    has_tu = bool(trip_updates)

    tu_by_seq, tu_by_sid = _build_tu_lookups(trip_updates) if has_tu else ({}, {})
    last_tu_delay = _last_known_tu_delay(trip_updates) if has_tu else None

    stops_sorted = sorted(all_trip_stops, key=lambda x: int(x.get("stop_sequence", 0)))

    nearest_idx = None
    pos_delay = None
    if has_position:
        nearest_idx = _find_nearest_stop(stops_sorted, vehicle_lat, vehicle_lon, stops_by_id)
        if nearest_idx is not None:
            pos_delay = _delay_from_position(stops_sorted, nearest_idx, now_s)

    fallback_delay = pos_delay if pos_delay is not None else last_tu_delay
    fallback_source = "estimated" if pos_delay is not None else (
        "estimated" if last_tu_delay is not None else "static"
    )

    # Indeks tego przystanku w posortowanej liście
    this_idx = next(
        (i for i, s in enumerate(stops_sorted) if int(s.get("stop_sequence", -1)) == stop_sequence),
        None,
    )

    # Status
    if nearest_idx is not None and this_idx is not None:
        if this_idx < nearest_idx:
            status = "passed"
        elif this_idx == nearest_idx:
            status = "current"
        else:
            status = "upcoming"
    else:
        status = "passed" if (sched_dep_s is not None and now_s > sched_dep_s + 60) else "upcoming"

    # Delay
    delay, source = _resolve_delay_for_stop(
        stop_sequence, str(stop_id), tu_by_seq, tu_by_sid,
        fallback_delay=fallback_delay,
        source_if_fallback=fallback_source,
    )

    # Zamrożenie przystanków 'passed' przy estymacji
    if status == "passed" and source != "trip_update":
        return {
            "status": status,
            "real_arrival": None,
            "real_departure": None,
            "delay_seconds": None,
            "source": "static",
        }

    real_arr = seconds_to_time(sched_arr_s + delay) if sched_arr_s is not None else None
    real_dep = seconds_to_time(sched_dep_s + delay) if sched_dep_s is not None else None

    return {
        "status": status,
        "real_arrival": real_arr,
        "real_departure": real_dep,
        "delay_seconds": delay,
        "source": source,
    }


# ---------------------------------------------------------------------------
# Endpointy
# ---------------------------------------------------------------------------

@require_GET
def get_stops_for_city(request):
    gtfs_loader.ensure_gtfs_loaded()

    city_name = request.GET.get("name")
    if not city_name:
        return error("Missing parameter: name")

    city = get_object_or_404(City, name=city_name)
    feeds = GTFSFeed.objects.filter(city=city, is_active=True)
    stops_map = {}

    for feed in feeds:
        feed_data = gtfs_loader.GTFS_DATA.get(feed.name)
        if not feed_data:
            continue

        for stop in feed_data.get("stops", []):
            try:
                lat = float(stop["stop_lat"])
                lon = float(stop["stop_lon"])
            except Exception:
                continue
            key = (round(lat, 5), round(lon, 5))
            if key not in stops_map:
                stops_map[key] = {
                    "stop_id": str(stop.get("stop_id")),
                    "stop_code": stop.get("stop_code"),
                    "stop_name": stop.get("stop_name"),
                    "lat": lat,
                    "lon": lon,
                    "routes": [],
                }

        trips = {t["trip_id"]: t for t in feed_data.get("trips", [])}
        routes = {r["route_id"]: r for r in feed_data.get("routes", [])}
        for st in feed_data.get("stop_times", []):
            sid = str(st.get("stop_id"))
            trip = trips.get(st.get("trip_id"))
            if not trip:
                continue
            route = routes.get(trip.get("route_id"))
            stop_record = next((s for s in feed_data.get("stops", []) if str(s.get("stop_id")) == sid), None)
            if not stop_record:
                continue
            try:
                lat = float(stop_record["stop_lat"])
                lon = float(stop_record["stop_lon"])
            except Exception:
                continue
            key = (round(lat, 5), round(lon, 5))
            entry = stops_map.get(key)
            if entry is None:
                continue
            route_info = {
                "feed": feed.name,
                "route_id": route.get("route_id") if route else None,
                "route_short_name": route.get("route_short_name") if route else None,
            }
            if route_info not in entry["routes"]:
                entry["routes"].append(route_info)

    return JsonResponse({"city": city.display_name, "count": len(stops_map), "stops": list(stops_map.values())})


@require_GET
def get_routes_for_city(request):
    gtfs_loader.ensure_gtfs_loaded()

    city_name = request.GET.get("name")
    if not city_name:
        return error("Missing parameter: name")

    city = get_object_or_404(City, name=city_name)
    feeds = GTFSFeed.objects.filter(city=city, is_active=True)
    routes = []

    for feed in feeds:
        feed_data = gtfs_loader.GTFS_DATA.get(feed.name)
        if not feed_data:
            continue
        for route in feed_data.get("routes", []):
            routes.append({
                "feed": feed.name,
                "route_id": route.get("route_id"),
                "route_short_name": route.get("route_short_name"),
                "color": route.get("route_color"),
                "text_color": route.get("route_text_color"),
            })

    return JsonResponse({"city": city.display_name, "routes": routes})


@require_GET
def get_schedule_for_stop(request):
    """
    Rozkład jazdy dla przystanku.

    Każdy odjazd zawiera teraz dodatkowe pola realtime:
        status        — 'passed' | 'current' | 'upcoming'
        real_arrival  — rzeczywisty czas przyjazdu (lub None)
        real_departure— rzeczywisty czas odjazdu (lub None)
        delay_seconds — opóźnienie w sek. (ujemne = przed czasem, None = brak danych)
        rt_source     — 'trip_update' | 'estimated' | 'static'

    Realtime jest ładowane raz na feed, nie na każdy kurs.
    """
    gtfs_loader.ensure_gtfs_loaded()

    city_name = request.GET.get("city")
    stop_id = request.GET.get("stop_id")
    if not city_name or not stop_id:
        return error("Missing parameters")

    stop_id = str(stop_id)
    city = get_object_or_404(City, name=city_name)
    feeds = GTFSFeed.objects.filter(city=city, is_active=True)
    result = []
    today = datetime.now().date()

    for feed in feeds:
        feed_data = gtfs_loader.GTFS_DATA.get(feed.name)
        if not feed_data:
            continue

        # Ładuj realtime raz dla całego feedu
        feed_obj = GTFSFeed.objects.filter(name=feed.name).first()
        realtime = load_realtime(feed_obj) if feed_obj else {}

        trips_by_id = {t["trip_id"]: t for t in feed_data.get("trips", [])}
        routes_by_id = {r["route_id"]: r for r in feed_data.get("routes", [])}
        services_by_id = {s["service_id"]: s for s in feed_data.get("calendar", [])}
        calendar_dates = feed_data.get("calendar_dates", [])
        service_dates_map = _build_service_dates_map(services_by_id, calendar_dates, today)

        # stops_by_id potrzebne do haversine w get_single_stop_realtime
        stops_by_id = {str(s["stop_id"]): s for s in feed_data.get("stops", [])}

        # Grupuj stop_times wg trip_id (potrzebne do get_single_stop_realtime)
        stop_times_by_trip: dict[str, list] = {}
        for st in feed_data.get("stop_times", []):
            tid = st.get("trip_id")
            if tid not in stop_times_by_trip:
                stop_times_by_trip[tid] = []
            stop_times_by_trip[tid].append(st)

        for st in feed_data.get("stop_times", []):
            if str(st.get("stop_id")) != stop_id:
                continue

            trip_id = st.get("trip_id")
            trip = trips_by_id.get(trip_id)
            if not trip:
                continue

            route = routes_by_id.get(trip.get("route_id"))
            service_id = trip.get("service_id")
            valid_dates = service_dates_map.get(service_id, [today])
            block_id = trip.get("brigade") or trip.get("block_id") or "N/A"

            sched_arr = st.get("arrival_time", "")
            sched_dep = st.get("departure_time", "")
            stop_seq = int(st.get("stop_sequence", 0))

            # Oblicz dane RT dla tego przystanku w tym kursie
            rt_info = get_single_stop_realtime(
                trip_id=trip_id,
                stop_id=stop_id,
                stop_sequence=stop_seq,
                sched_arr=sched_arr,
                sched_dep=sched_dep,
                realtime=realtime,
                all_trip_stops=stop_times_by_trip.get(trip_id, []),
                stops_by_id=stops_by_id,
            )

            for dt in valid_dates:
                result.append({
                    "feed": feed.name,
                    "trip_id": trip_id,
                    "route_id": trip.get("route_id"),
                    "route_short_name": route.get("route_short_name") if route else None,
                    "headsign": trip.get("trip_headsign"),
                    "arrival_time": sched_arr,
                    "departure_time": sched_dep,
                    "block_id": block_id,
                    "date": dt.strftime("%Y%m%d"),
                    # --- realtime ---
                    "status": rt_info["status"],
                    "real_arrival": rt_info["real_arrival"],
                    "real_departure": rt_info["real_departure"],
                    "delay_seconds": rt_info["delay_seconds"],
                    "rt_source": rt_info["source"],
                })

    return JsonResponse({"city": city.display_name, "stop_id": stop_id, "schedule": result})


@require_GET
def get_trip_details(request):
    """
    Zwraca wszystkie przystanki kursu + kształt + realtime.

    realtime.stop_times jest zawsze wypełnione (TripUpdates / estymacja / statik).
    Obsługuje niekompletne TripUpdates — dla brakujących stopów uzupełnia delay
    z pozycji pojazdu lub ostatniego znanego delay z TripUpdates.
    Ujemny delay_seconds oznacza jazdę przed czasem.
    Przystanki 'passed' z estymacją mają real_*=None, delay=None.
    """
    gtfs_loader.ensure_gtfs_loaded()

    city = request.GET.get("city")
    trip_id = request.GET.get("trip_id")
    feed_name = request.GET.get("feed_name")
    date = request.GET.get("date")

    if not all([city, trip_id, feed_name, date]):
        return error("Missing parameters")

    feed_data = gtfs_loader.GTFS_DATA.get(feed_name)
    if not feed_data:
        return error("Feed not loaded", 404)

    static_stops = sorted(
        [st for st in feed_data.get("stop_times", []) if st.get("trip_id") == trip_id],
        key=lambda x: int(x.get("stop_sequence", 0)),
    )

    trip_obj = next((t for t in feed_data.get("trips", []) if t.get("trip_id") == trip_id), None)
    shape_id = trip_obj.get("shape_id") if trip_obj else None
    shape = [s for s in feed_data.get("shapes", []) if s.get("shape_id") == shape_id]

    route_id = trip_obj.get("route_id") if trip_obj else None
    block_id = (trip_obj.get("brigade") or trip_obj.get("block_id") or "N/A") if trip_obj else "N/A"

    stops_by_id = {str(s["stop_id"]): s for s in feed_data.get("stops", [])}

    feed_obj = GTFSFeed.objects.filter(name=feed_name).first()
    realtime = load_realtime(feed_obj) if feed_obj else {}

    vehicle_data = extract_vehicle_for_trip(realtime, trip_id)
    trip_updates = extract_trip_updates_for_trip(realtime, trip_id)

    rich_stop_times = build_stop_times_with_realtime(
        static_stops=static_stops,
        trip_updates=trip_updates,
        vehicle_lat=vehicle_data.get("lat"),
        vehicle_lon=vehicle_data.get("lon"),
        stops_by_id=stops_by_id,
    )

    return JsonResponse({
        "trip_id": trip_id,
        "date": date,
        "route_id": route_id,
        "block_id": block_id,
        "headsign": trip_obj.get("trip_headsign") if trip_obj else None,
        "stops": static_stops,
        "shape": shape,
        "realtime": {
            "vehicle_number": vehicle_data.get("vehicle_number"),
            "lat": vehicle_data.get("lat"),
            "lon": vehicle_data.get("lon"),
            "stop_times": rich_stop_times,
        },
    })


@require_GET
def get_route_details(request):
    gtfs_loader.ensure_gtfs_loaded()

    city = request.GET.get("city")
    route_id = request.GET.get("route_id")
    feed_name = request.GET.get("feed_name")

    if not all([city, route_id, feed_name]):
        return error("Missing parameters")

    feed_data = gtfs_loader.GTFS_DATA.get(feed_name)
    if not feed_data:
        return error("Feed not loaded", 404)

    route = next((r for r in feed_data.get("routes", []) if r.get("route_id") == route_id), None)
    if not route:
        return error("Route not found", 404)

    directions = {"0": {"headsign": None, "stops": [], "shape": []},
                  "1": {"headsign": None, "stops": [], "shape": []}}
    trips_for_route = [t for t in feed_data.get("trips", []) if t.get("route_id") == route_id]

    for direction_id in ["0", "1"]:
        dir_trips = [t for t in trips_for_route if str(t.get("direction_id")) == str(direction_id)]
        if not dir_trips:
            continue
        directions[direction_id]["headsign"] = dir_trips[0].get("trip_headsign")
        first_trip_id = dir_trips[0].get("trip_id")
        directions[direction_id]["stops"] = [
            st for st in feed_data.get("stop_times", []) if st.get("trip_id") == first_trip_id
        ]
        shape_id = dir_trips[0].get("shape_id")
        directions[direction_id]["shape"] = [
            s for s in feed_data.get("shapes", []) if s.get("shape_id") == shape_id
        ]

    return JsonResponse({
        "city": city,
        "feed": feed_name,
        "route_id": route_id,
        "route_short_name": route.get("route_short_name"),
        "color": route.get("route_color"),
        "text_color": route.get("route_text_color"),
        "directions": directions,
    })


@require_GET
def get_block_schedule_for_route(request):
    gtfs_loader.ensure_gtfs_loaded()

    city = request.GET.get("city")
    route_id = request.GET.get("route_id")
    feed_name = request.GET.get("feed_name")

    if not all([city, route_id, feed_name]):
        return error("Missing parameters")

    feed_data = gtfs_loader.GTFS_DATA.get(feed_name)
    if not feed_data:
        return error("Feed not loaded", 404)

    trips_for_route = [t for t in feed_data.get("trips", []) if t.get("route_id") == route_id]
    if not trips_for_route:
        return error("No trips for this route", 404)

    today = datetime.now().date()
    services_by_id = {s["service_id"]: s for s in feed_data.get("calendar", [])}
    calendar_dates = feed_data.get("calendar_dates", [])
    service_dates_map = _build_service_dates_map(services_by_id, calendar_dates, today)

    block_map = {}
    for trip in trips_for_route:
        block_id = trip.get("brigade") or trip.get("block_id") or "N/A"
        service_id = trip.get("service_id")
        valid_dates = service_dates_map.get(service_id, [today])
        if block_id not in block_map:
            block_map[block_id] = {}
        for dt in valid_dates:
            dt_str = dt.strftime("%Y%m%d")
            if dt_str not in block_map[block_id]:
                block_map[block_id][dt_str] = []
            block_map[block_id][dt_str].append(trip.get("trip_id"))

    return JsonResponse({"city": city, "feed": feed_name, "route_id": route_id, "blocks": block_map})


@require_GET
def get_theoritical_block_details(request):
    gtfs_loader.ensure_gtfs_loaded()

    city = request.GET.get("city")
    block_id_param = request.GET.get("block_id")
    feed_name = request.GET.get("feed_name")
    date_param = request.GET.get("date")

    if not all([city, block_id_param, feed_name]):
        return error("Missing parameters")

    feed_data = gtfs_loader.GTFS_DATA.get(feed_name)
    if not feed_data:
        return error("Feed not loaded", 404)

    today = datetime.now().date()
    selected_date = None
    if date_param:
        try:
            selected_date = datetime.strptime(date_param, "%Y%m%d").date()
        except Exception:
            return error("Invalid date format, expected YYYYMMDD")

    trips = feed_data.get("trips", [])
    stop_times = feed_data.get("stop_times", [])
    routes_by_id = {r["route_id"]: r for r in feed_data.get("routes", [])}
    services_by_id = {s["service_id"]: s for s in feed_data.get("calendar", [])}
    calendar_dates = feed_data.get("calendar_dates", [])
    service_dates_map = _build_service_dates_map(services_by_id, calendar_dates, today)

    matched_trips = [t for t in trips if (t.get("brigade") or t.get("block_id")) == block_id_param]
    if not matched_trips:
        return error("No trips found for given block_id / brigade", 404)

    result_by_date = {}
    for trip in matched_trips:
        service_id = trip.get("service_id")
        valid_dates = service_dates_map.get(service_id, [])
        for d in valid_dates:
            if selected_date and d != selected_date:
                continue
            date_key = d.strftime("%Y%m%d")
            if date_key not in result_by_date:
                result_by_date[date_key] = {"date": date_key, "courses": []}
            trip_stop_times = sorted(
                [st for st in stop_times if st.get("trip_id") == trip.get("trip_id")],
                key=lambda x: int(x.get("stop_sequence", 0)),
            )
            if not trip_stop_times:
                continue
            route = routes_by_id.get(trip.get("route_id"))
            result_by_date[date_key]["courses"].append({
                "trip_id": trip.get("trip_id"),
                "route_id": trip.get("route_id"),
                "route_short_name": route.get("route_short_name") if route else None,
                "headsign": trip.get("trip_headsign"),
                "start_time": trip_stop_times[0].get("departure_time"),
                "end_time": trip_stop_times[-1].get("arrival_time"),
                "stops": trip_stop_times,
            })

    for day in result_by_date.values():
        day["courses"].sort(key=lambda c: c["start_time"])
        if day["courses"]:
            day["start_time"] = day["courses"][0]["start_time"]
            day["end_time"] = day["courses"][-1]["end_time"]
        else:
            day["start_time"] = None
            day["end_time"] = None

    return JsonResponse({
        "city": city,
        "feed": feed_name,
        "block_id": block_id_param,
        "dates": list(result_by_date.values()),
    })


@require_GET
def get_blocks_for_feed_and_date(request):
    gtfs_loader.ensure_gtfs_loaded()

    city = request.GET.get("city")
    feed_name = request.GET.get("feed_name")
    date_param = request.GET.get("date")

    if not all([city, feed_name, date_param]):
        return error("Missing parameters")

    try:
        selected_date = datetime.strptime(date_param, "%Y%m%d").date()
    except Exception:
        return error("Invalid date format, expected YYYYMMDD")

    feed_data = gtfs_loader.GTFS_DATA.get(feed_name)
    if not feed_data:
        return error("Feed not loaded", 404)

    trips = feed_data.get("trips", [])
    routes_by_id = {r["route_id"]: r for r in feed_data.get("routes", [])}
    services_by_id = {s["service_id"]: s for s in feed_data.get("calendar", [])}
    calendar_dates = feed_data.get("calendar_dates", [])
    service_dates_map = _build_service_dates_map(services_by_id, calendar_dates, selected_date)

    # Mapa trip_id → czas odjazdu z pierwszego przystanku (w sekundach).
    # Używamy departure_time przy stop_sequence=0 (lub najniższej sekwencji).
    # Pozwala to posortować kursy w bloku chronologicznie.
    first_departure_by_trip: dict[str, int] = {}
    # Grupujemy stop_times wg trip_id, żeby znaleźć minimum stop_sequence
    trip_first_stop: dict[str, dict] = {}
    for st in feed_data.get("stop_times", []):
        tid = st.get("trip_id")
        seq = int(st.get("stop_sequence", 0))
        if tid not in trip_first_stop or seq < int(trip_first_stop[tid].get("stop_sequence", 0)):
            trip_first_stop[tid] = st
    for tid, st in trip_first_stop.items():
        dep = time_to_seconds(st.get("departure_time", ""))
        if dep is not None:
            first_departure_by_trip[tid] = dep

    block_map: dict[str, list] = {}
    for trip in trips:
        service_id = trip.get("service_id")
        valid_dates = service_dates_map.get(service_id, [])
        if selected_date not in valid_dates:
            continue
        block_id = trip.get("brigade") or trip.get("block_id") or "N/A"
        if block_id not in block_map:
            block_map[block_id] = []
        route = routes_by_id.get(trip.get("route_id"))
        tid = trip.get("trip_id")
        block_map[block_id].append({
            "trip_id": tid,
            "route_id": trip.get("route_id"),
            "route_short_name": route.get("route_short_name") if route else None,
            "headsign": trip.get("trip_headsign"),
            # _sort_key jest tymczasowy — usuwamy go przed zwróceniem
            "_sort_key": first_departure_by_trip.get(tid, 0),
        })

    # Sortuj kursy w każdym bloku rosnąco wg czasu odjazdu z pierwszego
    # przystanku, a następnie nadaj pole `order` (0-based index).
    for block_id, course_list in block_map.items():
        course_list.sort(key=lambda c: c["_sort_key"])
        for idx, course in enumerate(course_list):
            course["order"] = idx
            del course["_sort_key"]

    return JsonResponse({"city": city, "feed": feed_name, "date": date_param, "blocks": block_map})


# ---------------------------------------------------------------------------
# Util — budowanie mapy dat kursowania (używana przez wiele endpointów)
# ---------------------------------------------------------------------------

def _build_service_dates_map(
    services_by_id: dict,
    calendar_dates: list,
    start_from: "date_type",
) -> dict:
    """Buduje mapę service_id → lista dat kursów (od start_from wzwyż)."""
    service_dates_map = {}

    for service_id, service in services_by_id.items():
        try:
            start_date = datetime.strptime(service["start_date"], "%Y%m%d").date()
            end_date = datetime.strptime(service["end_date"], "%Y%m%d").date()
        except Exception:
            continue
        days = [int(service.get(d, 0)) for d in
                ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]]
        dates = []
        current = max(start_from, start_date)
        while current <= end_date:
            if days[current.weekday()]:
                dates.append(current)
            current += timedelta(days=1)
        service_dates_map[service_id] = dates

    for cd in calendar_dates:
        sid = cd.get("service_id")
        try:
            d = datetime.strptime(cd.get("date"), "%Y%m%d").date()
        except Exception:
            continue
        if sid not in service_dates_map:
            service_dates_map[sid] = []
        if cd.get("exception_type") in ("1", "1\n"):
            if d not in service_dates_map[sid]:
                service_dates_map[sid].append(d)
        elif cd.get("exception_type") in ("2", "2\n"):
            if d in service_dates_map[sid]:
                service_dates_map[sid].remove(d)

    return service_dates_map