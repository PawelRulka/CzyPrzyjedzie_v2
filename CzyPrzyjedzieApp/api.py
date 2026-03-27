# CzyPrzyjedzieApp/api.py
import functools
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, date as date_type
from threading import Lock

import requests
from django.core.cache import cache
from django.http import JsonResponse
from django.shortcuts import get_object_or_404
from django.views.decorators.http import require_GET
from google.transit import gtfs_realtime_pb2

from . import gtfs_loader
from .models import City, GTFSFeed

# ---------------------------------------------------------------------------
# Globale – preindeksowane dane GTFS (budowane przy pierwszym użyciu)
# ---------------------------------------------------------------------------
_FEED_INDEXES = {}  # feed_name -> dict z indeksami
_FEED_INDEX_LOCK = Lock()


def _ensure_feed_indexes(feed_name):
    """Buduje i zwraca indeksy dla danego feedu (jeśli jeszcze nie istnieją)."""
    with _FEED_INDEX_LOCK:
        if feed_name in _FEED_INDEXES:
            return _FEED_INDEXES[feed_name]

        feed_data = gtfs_loader.GTFS_DATA.get(feed_name)
        if not feed_data:
            return None

        stops_by_id = {str(s["stop_id"]): s for s in feed_data.get("stops", [])}
        routes_by_id = {r["route_id"]: r for r in feed_data.get("routes", [])}
        trips_by_id = {t["trip_id"]: t for t in feed_data.get("trips", [])}

        stop_times_by_trip = {}
        stop_times_by_stop = {}
        for st in feed_data.get("stop_times", []):
            tid = st.get("trip_id")
            stop_times_by_trip.setdefault(tid, []).append(st)
            sid = str(st.get("stop_id"))
            stop_times_by_stop.setdefault(sid, []).append(st)

        for tid, sts in stop_times_by_trip.items():
            sts.sort(key=lambda x: int(x.get("stop_sequence", 0)))

        shapes_by_id = {}
        for s in feed_data.get("shapes", []):
            sid = s.get("shape_id")
            shapes_by_id.setdefault(sid, []).append(s)

        services_by_id = {s["service_id"]: s for s in feed_data.get("calendar", [])}
        calendar_dates = feed_data.get("calendar_dates", [])

        indexes = {
            "stops_by_id": stops_by_id,
            "routes_by_id": routes_by_id,
            "trips_by_id": trips_by_id,
            "stop_times_by_trip": stop_times_by_trip,
            "stop_times_by_stop": stop_times_by_stop,
            "shapes_by_id": shapes_by_id,
            "services_by_id": services_by_id,
            "calendar_dates": calendar_dates,
        }
        _FEED_INDEXES[feed_name] = indexes
        return indexes


# ---------------------------------------------------------------------------
# Cache dla dat kursowania – na poziomie feedu
# ---------------------------------------------------------------------------
_SERVICE_DATES_CACHE = {}  # (feed_name, start_date_iso) -> dict[service_id, list[date]]
_SERVICE_DATES_LOCK = Lock()


def get_service_dates_map(feed_name, start_date):
    key = (feed_name, start_date.isoformat())
    with _SERVICE_DATES_LOCK:
        if key in _SERVICE_DATES_CACHE:
            return _SERVICE_DATES_CACHE[key]

    indexes = _ensure_feed_indexes(feed_name)
    if not indexes:
        return {}

    services_by_id = indexes["services_by_id"]
    calendar_dates = indexes["calendar_dates"]
    service_dates_map = {}

    for service_id, service in services_by_id.items():
        try:
            start = datetime.strptime(service["start_date"], "%Y%m%d").date()
            end = datetime.strptime(service["end_date"], "%Y%m%d").date()
        except Exception:
            continue
        days = [int(service.get(d, 0)) for d in
                ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]]
        dates = []
        current = max(start_date, start)
        while current <= end:
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

    with _SERVICE_DATES_LOCK:
        _SERVICE_DATES_CACHE[key] = service_dates_map

    return service_dates_map


# ---------------------------------------------------------------------------
# Cache'owane funkcje pomocnicze
# ---------------------------------------------------------------------------
@functools.lru_cache(maxsize=2048)
def time_to_seconds_cached(time_str: str) -> int | None:
    if not time_str:
        return None
    try:
        h, m, s = map(int, time_str.split(":"))
        return h * 3600 + m * 60 + s
    except Exception:
        return None


@functools.lru_cache(maxsize=2048)
def seconds_to_time_cached(secs: int) -> str:
    secs = int(secs)
    h = secs // 3600
    m = (secs % 3600) // 60
    s = secs % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


@functools.lru_cache(maxsize=4096)
def haversine_cached(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    if lat1 == lat2 and lon1 == lon2:
        return 0.0
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


time_to_seconds = time_to_seconds_cached
seconds_to_time = seconds_to_time_cached
haversine = haversine_cached


# ---------------------------------------------------------------------------
# Realtime – cache z TTL
# ---------------------------------------------------------------------------
_rt_cache: dict[str, dict] = {}
_rt_cache_lock = Lock()
RT_CACHE_TTL = 20

_rt_index_cache: dict[str, tuple[dict, dict]] = {}
_rt_index_cache_lock = Lock()


def load_realtime_cached(feed: GTFSFeed) -> dict:
    now = time.monotonic()
    with _rt_cache_lock:
        cached = _rt_cache.get(feed.name)
        if cached and (now - cached["ts"]) < RT_CACHE_TTL:
            return cached["data"]
    data = load_realtime(feed)
    with _rt_cache_lock:
        _rt_cache[feed.name] = {"data": data, "ts": time.monotonic()}
    return data


def index_realtime_cached(feed_name: str, realtime: dict) -> tuple[dict, dict]:
    with _rt_index_cache_lock:
        cached = _rt_index_cache.get(feed_name)
        if cached:
            return cached

    vehicles_idx, updates_idx = index_realtime(realtime)
    with _rt_index_cache_lock:
        _rt_index_cache[feed_name] = (vehicles_idx, updates_idx)
    return vehicles_idx, updates_idx


# ---------------------------------------------------------------------------
# Helpery estymacji
# ---------------------------------------------------------------------------
def _find_nearest_stop(stops, vehicle_lat, vehicle_lon, stops_by_id):
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


def _delay_from_position(stops, nearest_idx, now_s):
    sched_dep = stops[nearest_idx].get("departure_time", "")
    sched_dep_s = time_to_seconds(sched_dep)
    if sched_dep_s is None:
        return 0
    return now_s - sched_dep_s


# ---------------------------------------------------------------------------
# Funkcje pomocnicze
# ---------------------------------------------------------------------------
def error(msg, status=400):
    return JsonResponse({"status": "error", "message": msg}, status=status)


def parse_date_from_time(date: datetime, time_str: str):
    h, m, s = map(int, time_str.split(":"))
    days_offset = h // 24
    h = h % 24
    return (date + timedelta(days=days_offset)).replace(hour=h, minute=m, second=s)


def _current_time_seconds() -> int:
    now = datetime.now()
    return now.hour * 3600 + now.minute * 60 + now.second


# ---------------------------------------------------------------------------
# Funkcje realtime
# ---------------------------------------------------------------------------
def load_realtime(feed: GTFSFeed) -> dict:
    urls = {
        "vehicle_positions": feed.vehicle_positions_url,
        "trip_updates": feed.trip_updates_url,
        "alerts": feed.service_alerts_url,
    }
    results = {k: None for k in urls}
    to_fetch = {k: v for k, v in urls.items() if v}
    if not to_fetch:
        return results
    with ThreadPoolExecutor(max_workers=len(to_fetch)) as executor:
        future_to_key = {
            executor.submit(_fetch_url, url): key
            for key, url in to_fetch.items()
        }
        for future in as_completed(future_to_key):
            key = future_to_key[future]
            try:
                raw = future.result()
                results[key] = _parse_feed(raw)
            except Exception:
                results[key] = None
    return results


def _fetch_url(url: str) -> bytes | dict | None:
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "")
        if url.endswith(".json") or "application/json" in content_type:
            return resp.json()
        return resp.content
    except Exception:
        return None


def _parse_feed(raw: bytes | dict | None):
    if raw is None:
        return None
    if isinstance(raw, (dict, list)):
        return raw
    feed_pb = gtfs_realtime_pb2.FeedMessage()
    try:
        feed_pb.ParseFromString(raw)
    except Exception:
        return None
    return feed_pb


def index_realtime(realtime: dict) -> tuple[dict, dict]:
    vehicles_idx = {}
    updates_idx = {}

    vp = realtime.get("vehicle_positions")
    if isinstance(vp, gtfs_realtime_pb2.FeedMessage):
        for entity in vp.entity:
            if entity.HasField("vehicle"):
                v = entity.vehicle
                tid = v.trip.trip_id
                if tid:
                    vehicles_idx[tid] = {
                        "vehicle_number": entity.id,
                        "lat": v.position.latitude,
                        "lon": v.position.longitude,
                    }
    elif vp is not None:
        entities = vp if isinstance(vp, list) else vp.get("entity", [])
        for entity in entities:
            v = entity.get("vehicle", {})
            tid = v.get("trip", {}).get("trip_id")
            if tid:
                pos = v.get("position", {})
                vehicles_idx[tid] = {
                    "vehicle_number": v.get("vehicle", {}).get("id") or entity.get("id"),
                    "lat": pos.get("latitude"),
                    "lon": pos.get("longitude"),
                }

    tu = realtime.get("trip_updates")
    if isinstance(tu, gtfs_realtime_pb2.FeedMessage):
        for entity in tu.entity:
            if entity.HasField("trip_update"):
                upd = entity.trip_update
                tid = upd.trip.trip_id
                if not tid:
                    continue
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
                updates_idx[tid] = result
    elif tu is not None:
        entities = tu if isinstance(tu, list) else tu.get("entity", [])
        for entity in entities:
            trip_update = entity.get("trip_update", {})
            tid = trip_update.get("trip", {}).get("trip_id")
            if not tid:
                continue
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
            updates_idx[tid] = result

    return vehicles_idx, updates_idx


def extract_vehicle_for_trip(realtime: dict, trip_id: str) -> dict:
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


def _build_tu_lookups(trip_updates: list) -> tuple[dict, dict]:
    by_seq = {}
    by_sid = {}
    for u in trip_updates:
        if u.get("stop_sequence") is not None:
            by_seq[int(u["stop_sequence"])] = u
        if u.get("stop_id") is not None:
            by_sid[str(u["stop_id"])] = u
    return by_seq, by_sid


def _last_known_tu_delay(trip_updates: list) -> int | None:
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


def _resolve_delay_for_stop(seq, sid, tu_by_seq, tu_by_sid, fallback_delay, source_if_fallback):
    update = tu_by_seq.get(seq) or tu_by_sid.get(sid)
    if update is not None:
        dep = update.get("departure_delay")
        arr = update.get("arrival_delay")
        delay = dep if dep is not None else (arr if arr is not None else 0)
        return delay, "trip_update"
    if fallback_delay is not None:
        return fallback_delay, source_if_fallback
    return 0, "static"


def _generate_shape_from_stops(stops: list, stops_by_id: dict) -> list:
    shape = []
    seq = 0
    dist = 0.0
    prev_lat = None
    prev_lon = None
    for st in sorted(stops, key=lambda x: int(x.get("stop_sequence", 0))):
        sid = str(st.get("stop_id", ""))
        info = stops_by_id.get(sid)
        if not info:
            continue
        try:
            lat = float(info["stop_lat"])
            lon = float(info["stop_lon"])
        except (KeyError, TypeError, ValueError):
            continue
        if prev_lat is not None and prev_lon is not None:
            dist += haversine(prev_lat, prev_lon, lat, lon)
        shape.append({
            "shape_id": "generated",
            "shape_pt_lat": lat,
            "shape_pt_lon": lon,
            "shape_pt_sequence": seq,
            "shape_dist_traveled": round(dist, 4),
            "generated": True,
        })
        prev_lat, prev_lon = lat, lon
        seq += 1
    return shape


def _vehicle_id_matches(candidate: str | None, query: str) -> bool:
    if not candidate:
        return False
    if candidate == query:
        return True
    bare = candidate.split("-")[0]
    if bare == query:
        return True
    if bare.rsplit("/", 1)[-1] == query:
        return True
    return False


def _headsign_or_last_stop(headsign: str | None, stops: list, stops_by_id: dict) -> str | None:
    if headsign:
        return headsign
    if not stops:
        return None
    last_stop = max(stops, key=lambda x: int(x.get("stop_sequence", 0)))
    info = stops_by_id.get(str(last_stop.get("stop_id", "")))
    return info.get("stop_name") if info else None


def _shape_or_fallback(shape: list, stops: list, stops_by_id: dict) -> list:
    if shape:
        return shape
    return _generate_shape_from_stops(stops, stops_by_id)


def _enrich_stops_with_track_platform(stops: list) -> list:
    """
    Zwraca kopię listy stop_times z jawnie ustawionymi polami track i platform
    (None jeśli nie ma ich w feedzie).
    Nie mutuje oryginalnych słowników ze współdzielonego indeksu.
    """
    return [
        {
            **st,
            "track": st.get("track"),
            "platform": st.get("platform"),
            "pickup_type": _normalize_pickup_dropoff(st.get("pickup_type")),
            "drop_off_type": _normalize_pickup_dropoff(st.get("drop_off_type")),
        }
        for st in stops
    ]


def _normalize_pickup_dropoff(value) -> str:
    v = "" if value is None else str(value).strip()
    return v if v != "" else "0"


def build_stop_times_with_realtime(
    static_stops: list,
    trip_updates: list,
    vehicle_lat: float | None,
    vehicle_lon: float | None,
    stops_by_id: dict,
) -> list:
    if not static_stops:
        return []
    stops = static_stops
    now_s = _current_time_seconds()
    has_position = vehicle_lat is not None and vehicle_lon is not None
    has_tu = bool(trip_updates)
    tu_by_seq, tu_by_sid = _build_tu_lookups(trip_updates) if has_tu else ({}, {})
    last_tu_delay = _last_known_tu_delay(trip_updates) if has_tu else None
    nearest_idx = None
    pos_delay = None
    if has_position:
        nearest_idx = _find_nearest_stop(stops, vehicle_lat, vehicle_lon, stops_by_id)
        if nearest_idx is not None:
            pos_delay = _delay_from_position(stops, nearest_idx, now_s)
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
        if nearest_idx is not None:
            if i < nearest_idx:
                status = "passed"
            elif i == nearest_idx:
                status = "current"
            else:
                status = "upcoming"
        else:
            if sched_dep_s is not None and now_s > sched_dep_s + 60:
                status = "passed"
            else:
                status = "upcoming"
        delay, source = _resolve_delay_for_stop(
            seq, sid, tu_by_seq, tu_by_sid,
            fallback_delay=fallback_delay,
            source_if_fallback=fallback_source,
        )
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
    vehicles_idx: dict,
    updates_idx: dict,
    all_trip_stops: list,
    stops_by_id: dict,
) -> dict:
    now_s = _current_time_seconds()
    sched_arr_s = time_to_seconds(sched_arr)
    sched_dep_s = time_to_seconds(sched_dep)
    vehicle_data = vehicles_idx.get(trip_id, {})
    trip_updates = updates_idx.get(trip_id, [])
    vehicle_lat = vehicle_data.get("lat")
    vehicle_lon = vehicle_data.get("lon")
    has_position = vehicle_lat is not None and vehicle_lon is not None
    has_tu = bool(trip_updates)
    tu_by_seq, tu_by_sid = _build_tu_lookups(trip_updates) if has_tu else ({}, {})
    last_tu_delay = _last_known_tu_delay(trip_updates) if has_tu else None
    stops_sorted = all_trip_stops
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
    this_idx = next(
        (i for i, s in enumerate(stops_sorted) if int(s.get("stop_sequence", -1)) == stop_sequence),
        None,
    )
    if nearest_idx is not None and this_idx is not None:
        if this_idx < nearest_idx:
            status = "passed"
        elif this_idx == nearest_idx:
            status = "current"
        else:
            status = "upcoming"
    else:
        status = "passed" if (sched_dep_s is not None and now_s > sched_dep_s + 60) else "upcoming"
    delay, source = _resolve_delay_for_stop(
        stop_sequence, str(stop_id), tu_by_seq, tu_by_sid,
        fallback_delay=fallback_delay,
        source_if_fallback=fallback_source,
    )
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

    cache_key = f"stops_for_city_{city_name}"
    cached = cache.get(cache_key)
    if cached:
        return JsonResponse(cached)

    city = get_object_or_404(City, name=city_name)
    feeds = GTFSFeed.objects.filter(city=city, is_active=True)
    stops_map = {}

    for feed in feeds:
        indexes = _ensure_feed_indexes(feed.name)
        if not indexes:
            continue
        feed_data = gtfs_loader.GTFS_DATA.get(feed.name)
        if not feed_data:
            continue

        stops_by_id = indexes["stops_by_id"]
        trips_by_id = {t["trip_id"]: t for t in feed_data.get("trips", [])}
        routes_by_id = {r["route_id"]: r for r in feed_data.get("routes", [])}

        for stop in stops_by_id.values():
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

        stop_times_by_stop = indexes["stop_times_by_stop"]
        for stop_id, st_list in stop_times_by_stop.items():
            route_ids = set()
            for st in st_list:
                trip = trips_by_id.get(st.get("trip_id"))
                if trip:
                    route_id = trip.get("route_id")
                    if route_id:
                        route_ids.add(route_id)
            for route_id in route_ids:
                route = routes_by_id.get(route_id)
                if not route:
                    continue
                stop_info = stops_by_id.get(stop_id)
                if not stop_info:
                    continue
                try:
                    lat = float(stop_info["stop_lat"])
                    lon = float(stop_info["stop_lon"])
                except Exception:
                    continue
                key = (round(lat, 5), round(lon, 5))
                entry = stops_map.get(key)
                if entry is None:
                    continue
                route_info = {
                    "feed": feed.name,
                    "route_id": route_id,
                    "route_short_name": route.get("route_short_name"),
                }
                if route_info not in entry["routes"]:
                    entry["routes"].append(route_info)

    response_data = {"city": city.display_name, "count": len(stops_map), "stops": list(stops_map.values())}
    cache.set(cache_key, response_data, 60)
    return JsonResponse(response_data)


@require_GET
def get_routes_for_city(request):
    gtfs_loader.ensure_gtfs_loaded()
    city_name = request.GET.get("name")
    if not city_name:
        return error("Missing parameter: name")

    cache_key = f"routes_for_city_{city_name}"
    cached = cache.get(cache_key)
    if cached:
        return JsonResponse(cached)

    city = get_object_or_404(City, name=city_name)
    feeds = GTFSFeed.objects.filter(city=city, is_active=True)
    routes = []

    for feed in feeds:
        indexes = _ensure_feed_indexes(feed.name)
        if not indexes:
            continue
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

    response_data = {"city": city.display_name, "routes": routes}
    cache.set(cache_key, response_data, 60)
    return JsonResponse(response_data)


@require_GET
def get_schedule_for_stop(request):
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
        indexes = _ensure_feed_indexes(feed.name)
        if not indexes:
            continue

        feed_obj = feed
        realtime = load_realtime_cached(feed_obj) if feed_obj else {}
        vehicles_idx, updates_idx = index_realtime_cached(feed.name, realtime)

        stop_times_for_stop = indexes["stop_times_by_stop"].get(stop_id, [])
        trips_by_id = indexes["trips_by_id"]
        routes_by_id = indexes["routes_by_id"]
        stops_by_id = indexes["stops_by_id"]
        service_dates_map = get_service_dates_map(feed.name, today)

        for st in stop_times_for_stop:
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
            trip_stops = indexes["stop_times_by_trip"].get(trip_id, [])
            max_trip_seq = max((int(ts.get("stop_sequence", 0)) for ts in trip_stops), default=stop_seq)
            is_last_stop = stop_seq >= max_trip_seq

            rt_info = get_single_stop_realtime(
                trip_id=trip_id,
                stop_id=stop_id,
                stop_sequence=stop_seq,
                sched_arr=sched_arr,
                sched_dep=sched_dep,
                vehicles_idx=vehicles_idx,
                updates_idx=updates_idx,
                all_trip_stops=indexes["stop_times_by_trip"].get(trip_id, []),
                stops_by_id=stops_by_id,
            )

            for dt in valid_dates:
                if dt == today:
                    status = rt_info["status"]
                    real_arrival = rt_info["real_arrival"]
                    real_departure = rt_info["real_departure"]
                    delay_seconds = rt_info["delay_seconds"]
                    rt_source = rt_info["source"]
                else:
                    status = "upcoming"
                    real_arrival = None
                    real_departure = None
                    delay_seconds = None
                    rt_source = "static"

                result.append({
                    "feed": feed.name,
                    "trip_id": trip_id,
                    "route_id": trip.get("route_id"),
                    "route_short_name": route.get("route_short_name") if route else None,
                    "headsign": trip.get("trip_headsign"),
                    "trip_short_name": trip.get("trip_short_name"),
                    "plk_train_name": trip.get("plk_train_name"),
                    "arrival_time": sched_arr,
                    "departure_time": sched_dep,
                    "track": st.get("track"),
                    "platform": st.get("platform"),
                    "pickup_type": _normalize_pickup_dropoff(st.get("pickup_type")),
                    "drop_off_type": _normalize_pickup_dropoff(st.get("drop_off_type")),
                    "block_id": block_id,
                    "is_last_stop": is_last_stop,
                    "date": dt.strftime("%Y%m%d"),
                    "status": status,
                    "real_arrival": real_arrival,
                    "real_departure": real_departure,
                    "delay_seconds": delay_seconds,
                    "rt_source": rt_source,
                })

    return JsonResponse({"city": city.display_name, "stop_id": stop_id, "schedule": result})


@require_GET
def get_trip_details(request):
    gtfs_loader.ensure_gtfs_loaded()
    city = request.GET.get("city")
    trip_id = request.GET.get("trip_id")
    feed_name = request.GET.get("feed_name")
    date = request.GET.get("date")

    if not all([city, feed_name, date]):
        return error("Missing parameters: city, feed_name and date are required")

    indexes = _ensure_feed_indexes(feed_name)
    if not indexes:
        return error("Feed not loaded", 404)

    feed_obj = GTFSFeed.objects.filter(name=feed_name).first()
    if not feed_obj:
        return error(f"Feed '{feed_name}' not found", 404)

    realtime = load_realtime_cached(feed_obj)
    vehicles_idx, updates_idx = index_realtime_cached(feed_name, realtime)

    stops_by_id = indexes["stops_by_id"]
    shapes_by_id = indexes["shapes_by_id"]
    stop_times_by_trip = indexes["stop_times_by_trip"]

    def _build_trip_payload(t_obj: dict) -> dict:
        tid = t_obj.get("trip_id")
        shape_id = t_obj.get("shape_id")
        block_id = t_obj.get("brigade") or t_obj.get("block_id") or "N/A"
        static_stops = stop_times_by_trip.get(tid, [])
        vehicle_data = vehicles_idx.get(tid, {})
        trip_updates = updates_idx.get(tid, [])
        rich_stop_times = build_stop_times_with_realtime(
            static_stops=static_stops,
            trip_updates=trip_updates,
            vehicle_lat=vehicle_data.get("lat"),
            vehicle_lon=vehicle_data.get("lon"),
            stops_by_id=stops_by_id,
        )
        shape = _shape_or_fallback(shapes_by_id.get(shape_id, []), static_stops, stops_by_id)
        return {
            "trip_id": tid,
            "date": date,
            "route_id": t_obj.get("route_id"),
            "block_id": block_id,
            "trip_short_name": t_obj.get("trip_short_name"),
            "plk_train_name": t_obj.get("plk_train_name"),
            "headsign": _headsign_or_last_stop(t_obj.get("trip_headsign"), static_stops, stops_by_id),
            "stops": _enrich_stops_with_track_platform(static_stops),
            "shape": shape,
            "realtime": {
                "vehicle_number": vehicle_data.get("vehicle_number"),
                "lat": vehicle_data.get("lat"),
                "lon": vehicle_data.get("lon"),
                "stop_times": rich_stop_times,
            },
        }

    if trip_id:
        trip_obj = indexes["trips_by_id"].get(trip_id)
        if not trip_obj:
            return error(f"Trip '{trip_id}' not found in feed '{feed_name}'", 404)
        return JsonResponse(_build_trip_payload(trip_obj))

    try:
        selected_date = datetime.strptime(date, "%Y%m%d").date()
    except ValueError:
        return error("Invalid date format, expected YYYYMMDD")

    service_dates_map = get_service_dates_map(feed_name, selected_date)
    active_trip_ids = set()
    for tid, trip in indexes["trips_by_id"].items():
        if selected_date in service_dates_map.get(trip.get("service_id"), []):
            active_trip_ids.add(tid)

    active_trips = [t for t in indexes["trips_by_id"].values() if t["trip_id"] in active_trip_ids]
    trips_payload = [_build_trip_payload(t) for t in active_trips]

    return JsonResponse({
        "city": city,
        "feed": feed_name,
        "date": date,
        "count": len(trips_payload),
        "trips": trips_payload,
    })


@require_GET
def get_line_brigade_delay_for_trip(request):
    gtfs_loader.ensure_gtfs_loaded()
    city = request.GET.get("city")
    trip_id = request.GET.get("trip_id")
    feed_name = request.GET.get("feed_name")
    date = request.GET.get("date")

    if not all([city, feed_name, date]):
        return error("Missing parameters: city, feed_name and date are required")

    indexes = _ensure_feed_indexes(feed_name)
    if not indexes:
        return error("Feed not loaded", 404)

    feed_obj = GTFSFeed.objects.filter(name=feed_name).first()
    if not feed_obj:
        return error(f"Feed '{feed_name}' not found", 404)

    realtime = load_realtime_cached(feed_obj)
    vehicles_idx, updates_idx = index_realtime_cached(feed_name, realtime)

    stops_by_id = indexes["stops_by_id"]
    stop_times_by_trip = indexes["stop_times_by_trip"]
    now_s = _current_time_seconds()

    def _build_delay_payload(t_obj: dict) -> dict:
        tid = t_obj.get("trip_id")
        block_id = t_obj.get("brigade") or t_obj.get("block_id") or "N/A"
        vehicle_data = vehicles_idx.get(tid, {})
        trip_updates = updates_idx.get(tid, [])
        has_position = vehicle_data.get("lat") is not None and vehicle_data.get("lon") is not None
        has_tu = bool(trip_updates)
        delay = None
        rt_source = None
        if has_tu:
            last_delay = _last_known_tu_delay(trip_updates)
            if last_delay is not None:
                delay = last_delay
                rt_source = "trip_update"
        if delay is None and has_position:
            stops_sorted = stop_times_by_trip.get(tid, [])
            nearest_idx = _find_nearest_stop(
                stops_sorted, vehicle_data["lat"], vehicle_data["lon"], stops_by_id
            )
            if nearest_idx is not None:
                delay = _delay_from_position(stops_sorted, nearest_idx, now_s)
                rt_source = "estimated"
        return {
            "trip_id": tid,
            "route_id": t_obj.get("route_id"),
            "block_id": block_id,
            "trip_short_name": t_obj.get("trip_short_name"),
            "delay_seconds": delay,
            "rt_source": rt_source,
        }

    if trip_id:
        trip_obj = indexes["trips_by_id"].get(trip_id)
        if not trip_obj:
            return error(f"Trip '{trip_id}' not found in feed '{feed_name}'", 404)
        return JsonResponse(_build_delay_payload(trip_obj))

    try:
        selected_date = datetime.strptime(date, "%Y%m%d").date()
    except ValueError:
        return error("Invalid date format, expected YYYYMMDD")

    service_dates_map = get_service_dates_map(feed_name, selected_date)
    active_trip_ids = set()
    for tid, trip in indexes["trips_by_id"].items():
        if selected_date in service_dates_map.get(trip.get("service_id"), []):
            active_trip_ids.add(tid)

    active_trips = [t for t in indexes["trips_by_id"].values() if t["trip_id"] in active_trip_ids]
    trips_payload = [_build_delay_payload(t) for t in active_trips]

    return JsonResponse({
        "city": city,
        "feed": feed_name,
        "date": date,
        "count": len(trips_payload),
        "trips": trips_payload,
    })


def _find_trip_update_block(realtime: dict, trip_id: str) -> dict | None:
    tu = realtime.get("trip_updates")
    if tu is None:
        return None

    if isinstance(tu, gtfs_realtime_pb2.FeedMessage):
        for entity in tu.entity:
            if entity.HasField("trip_update") and entity.trip_update.trip.trip_id == trip_id:
                upd = entity.trip_update
                stu_list = []
                for stu in upd.stop_time_update:
                    stu_list.append({
                        "stop_id": stu.stop_id,
                        "stop_sequence": stu.stop_sequence,
                        "arrival": {"delay": stu.arrival.delay, "time": stu.arrival.time}
                            if stu.HasField("arrival") else {},
                        "departure": {"delay": stu.departure.delay, "time": stu.departure.time}
                            if stu.HasField("departure") else {},
                    })
                return {"trip": {"tripId": trip_id}, "stopTimeUpdate": stu_list}
        return None

    entities = tu if isinstance(tu, list) else tu.get("entity", [])
    for entity in entities:
        tu_block = entity.get("tripUpdate") or entity.get("trip_update") or {}
        trip_info = tu_block.get("trip", {})
        tid = trip_info.get("tripId") or trip_info.get("trip_id")
        if tid == trip_id:
            return tu_block

    return None


@require_GET
def get_trip_by_vehicle(request):
    gtfs_loader.ensure_gtfs_loaded()
    feed_name = request.GET.get("feed_name")
    vehicle_id = request.GET.get("vehicle_id")

    if not all([feed_name, vehicle_id]):
        return error("Missing parameters: feed_name and vehicle_id are required")

    indexes = _ensure_feed_indexes(feed_name)
    if not indexes:
        return error("Feed not loaded", 404)

    feed_obj = GTFSFeed.objects.filter(name=feed_name).first()
    if not feed_obj:
        return error(f"Feed '{feed_name}' not found", 404)

    realtime = load_realtime_cached(feed_obj)

    trip_id = None
    vehicle_lat = None
    vehicle_lon = None
    vehicle_number = None

    vp = realtime.get("vehicle_positions")
    if isinstance(vp, gtfs_realtime_pb2.FeedMessage):
        for entity in vp.entity:
            if not entity.HasField("vehicle"):
                continue
            v = entity.vehicle
            if _vehicle_id_matches(v.vehicle.id, vehicle_id) or _vehicle_id_matches(entity.id, vehicle_id):
                trip_id = v.trip.trip_id or None
                vehicle_lat = v.position.latitude or None
                vehicle_lon = v.position.longitude or None
                vehicle_number = entity.id
                break
    elif vp is not None:
        entities = vp if isinstance(vp, list) else vp.get("entity", [])
        for entity in entities:
            v = entity.get("vehicle", {})
            veh_info = v.get("vehicle", {})
            if _vehicle_id_matches(veh_info.get("id"), vehicle_id) or _vehicle_id_matches(entity.get("id"), vehicle_id):
                trip_id = v.get("trip", {}).get("trip_id")
                pos = v.get("position", {})
                vehicle_lat = pos.get("latitude")
                vehicle_lon = pos.get("longitude")
                vehicle_number = veh_info.get("id") or entity.get("id")
                break

    is_estimated = False
    estimated_delay_seconds: int | None = None
    _est_result: dict | None = None
    _est_tu_block: dict | None = None
    _est_now_ts: int | None = None

    if not trip_id:
        est_prefix = "EST-"
        if vehicle_id.startswith(est_prefix):
            candidate_trip_id = vehicle_id[len(est_prefix):]
            tu_block = _find_trip_update_block(realtime, candidate_trip_id)
            if tu_block is not None:
                now_ts = int(time.time())
                if (not _is_trip_finished(candidate_trip_id, tu_block, indexes, now_ts=now_ts)
                        and not _is_trip_not_yet_started(candidate_trip_id, indexes, now_ts=now_ts)):
                    est = _estimate_position_from_trip_update(
                        feed_name, candidate_trip_id, tu_block, indexes, now_ts=now_ts
                    )
                    if est:
                        trip_id = candidate_trip_id
                        vehicle_lat = est["lat"]
                        vehicle_lon = est["lon"]
                        vehicle_number = vehicle_id
                        is_estimated = True
                        _est_result = est
                        _est_tu_block = tu_block
                        _est_now_ts = now_ts

    if not trip_id:
        return JsonResponse({
            "feed_name": feed_name,
            "vehicle_id": vehicle_id,
            "found": False,
            "message": "Pojazd nie jest aktualnie aktywny lub nie ma danych w VehiclePositions.",
        }, status=404)

    trip_obj = indexes["trips_by_id"].get(trip_id)
    static_stops = indexes["stop_times_by_trip"].get(trip_id, [])
    shape_id = trip_obj.get("shape_id") if trip_obj else None
    raw_shape = indexes["shapes_by_id"].get(shape_id, [])
    route_id = trip_obj.get("route_id") if trip_obj else None
    block_id = (trip_obj.get("brigade") or trip_obj.get("block_id") or "N/A") if trip_obj else "N/A"
    stops_by_id = indexes["stops_by_id"]
    trip_updates = extract_trip_updates_for_trip(realtime, trip_id)

    if is_estimated and _est_result is not None and _est_now_ts is not None:
        estimated_delay_seconds = _estimate_delay_from_position(
            est=_est_result,
            static_stops=static_stops,
            now_ts=_est_now_ts,
        )

    rich_stop_times = build_stop_times_with_realtime(
        static_stops=static_stops,
        trip_updates=trip_updates,
        vehicle_lat=vehicle_lat,
        vehicle_lon=vehicle_lon,
        stops_by_id=stops_by_id,
    )
    shape = _shape_or_fallback(raw_shape, static_stops, stops_by_id)

    return JsonResponse({
        "trip_id": trip_id,
        "route_id": route_id,
        "block_id": block_id,
        "trip_short_name": trip_obj.get("trip_short_name") if trip_obj else None,
        "plk_train_name": trip_obj.get("plk_train_name") if trip_obj else None,
        "headsign": _headsign_or_last_stop(
            trip_obj.get("trip_headsign") if trip_obj else None,
            static_stops,
            stops_by_id,
        ),
        "stops": _enrich_stops_with_track_platform(static_stops),
        "shape": shape,
        "positionType": "estimated" if is_estimated else "real",
        "estimatedDelay": {
            "delay_seconds": estimated_delay_seconds,
            "source": "position_interpolation" if estimated_delay_seconds is not None else None,
        } if is_estimated else None,
        "realtime": {
            "vehicle_number": vehicle_number,
            "lat": vehicle_lat,
            "lon": vehicle_lon,
            "stop_times": rich_stop_times,
        },
    })


@require_GET
def get_route_details(request):
    gtfs_loader.ensure_gtfs_loaded()
    city = request.GET.get("city")
    route_id = request.GET.get("route_id")
    feed_name = request.GET.get("feed_name")

    if not all([city, feed_name]):
        return error("Missing parameters: city and feed_name are required")

    indexes = _ensure_feed_indexes(feed_name)
    if not indexes:
        return error("Feed not loaded", 404)

    if not route_id:
        routes = [
            {
                "city": city,
                "feed": feed_name,
                "route_id": r.get("route_id"),
                "route_short_name": r.get("route_short_name"),
                "route_type": r.get("route_type"),
                "color": r.get("route_color"),
                "text_color": r.get("route_text_color"),
            }
            for r in gtfs_loader.GTFS_DATA.get(feed_name, {}).get("routes", [])
        ]
        return JsonResponse({"city": city, "feed": feed_name, "count": len(routes), "routes": routes})

    route = indexes["routes_by_id"].get(route_id)
    if not route:
        return error("Route not found", 404)

    stops_by_id = indexes["stops_by_id"]
    trips_for_route = [t for t in indexes["trips_by_id"].values() if t.get("route_id") == route_id]

    # --- Helpery lokalne ---

    def _effective_headsign(trip: dict) -> str | None:
        """Zwraca headsign kursu lub nazwę ostatniego przystanku jako fallback."""
        hs = trip.get("trip_headsign")
        if hs:
            return hs
        stops = indexes["stop_times_by_trip"].get(trip.get("trip_id"), [])
        if not stops:
            return None
        last = max(stops, key=lambda x: int(x.get("stop_sequence", 0)))
        info = stops_by_id.get(str(last.get("stop_id", "")))
        return info.get("stop_name") if info else None

    def _build_direction_entry(rep_trip: dict) -> dict:
        """Buduje wpis directions na podstawie reprezentatywnego kursu."""
        tid = rep_trip.get("trip_id")
        shape_id = rep_trip.get("shape_id")
        dir_stops = indexes["stop_times_by_trip"].get(tid, [])
        raw_shape = indexes["shapes_by_id"].get(shape_id, [])
        return {
            "headsign": _effective_headsign(rep_trip),
            "stops": _enrich_stops_with_track_platform(dir_stops),
            "shape": _shape_or_fallback(raw_shape, dir_stops, stops_by_id),
        }

    def _most_common_combo(trips: list) -> tuple | None:
        """Zwraca najczęstszą parę (headsign, shape_id) z listy kursów."""
        counts: dict[tuple, int] = {}
        for t in trips:
            key = (_effective_headsign(t), t.get("shape_id"))
            counts[key] = counts.get(key, 0) + 1
        return max(counts, key=lambda k: counts[k]) if counts else None

    # --- Budowanie directions ---

    directions: dict[str, dict] = {
        "0": {"headsign": None, "stops": [], "shape": []},
        "1": {"headsign": None, "stops": [], "shape": []},
    }

    has_direction_id = any(
        str(t.get("direction_id", "")).strip() != ""
        for t in trips_for_route
    )

    if has_direction_id:
        # Przypadek 1: trips.txt zawiera direction_id
        # Dla każdego kierunku (0/1) szukamy najczęstszej pary (headsign, shape_id)
        for direction_id in ["0", "1"]:
            dir_trips = [
                t for t in trips_for_route
                if str(t.get("direction_id", "")).strip() == direction_id
            ]
            if not dir_trips:
                continue
            best_combo = _most_common_combo(dir_trips)
            if best_combo is None:
                continue
            rep_trip = next(
                t for t in dir_trips
                if (_effective_headsign(t), t.get("shape_id")) == best_combo
            )
            directions[direction_id] = _build_direction_entry(rep_trip)
    else:
        # Przypadek 2/3: brak direction_id – szukamy 2 najczęstszych par (headsign, shape_id)
        counts: dict[tuple, int] = {}
        for t in trips_for_route:
            key = (_effective_headsign(t), t.get("shape_id"))
            counts[key] = counts.get(key, 0) + 1
        top_two = sorted(counts, key=lambda k: counts[k], reverse=True)[:2]
        for slot, combo in enumerate(top_two):
            rep_trip = next(
                t for t in trips_for_route
                if (_effective_headsign(t), t.get("shape_id")) == combo
            )
            directions[str(slot)] = _build_direction_entry(rep_trip)

    return JsonResponse({
        "city": city,
        "feed": feed_name,
        "route_id": route_id,
        "route_short_name": route.get("route_short_name"),
        "route_type": route.get("route_type"),
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

    indexes = _ensure_feed_indexes(feed_name)
    if not indexes:
        return error("Feed not loaded", 404)

    trips_for_route = [t for t in indexes["trips_by_id"].values() if t.get("route_id") == route_id]
    if not trips_for_route:
        return error("No trips for this route", 404)

    today = datetime.now().date()
    service_dates_map = get_service_dates_map(feed_name, today)

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

    indexes = _ensure_feed_indexes(feed_name)
    if not indexes:
        return error("Feed not loaded", 404)

    today = datetime.now().date()
    selected_date = None
    if date_param:
        try:
            selected_date = datetime.strptime(date_param, "%Y%m%d").date()
        except Exception:
            return error("Invalid date format, expected YYYYMMDD")

    trips = indexes["trips_by_id"].values()
    stop_times_by_trip = indexes["stop_times_by_trip"]
    routes_by_id = indexes["routes_by_id"]
    service_dates_map = get_service_dates_map(feed_name, today)

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
            trip_stop_times = stop_times_by_trip.get(trip.get("trip_id"), [])
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
                "stops": _enrich_stops_with_track_platform(trip_stop_times),
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

    indexes = _ensure_feed_indexes(feed_name)
    if not indexes:
        return error("Feed not loaded", 404)

    trips = indexes["trips_by_id"].values()
    routes_by_id = indexes["routes_by_id"]
    service_dates_map = get_service_dates_map(feed_name, selected_date)

    first_departure_by_trip = {}
    for tid, sts in indexes["stop_times_by_trip"].items():
        if sts:
            dep = time_to_seconds(sts[0].get("departure_time", ""))
            if dep is not None:
                first_departure_by_trip[tid] = dep

    block_map = {}
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
        stop_times = indexes["stop_times_by_trip"].get(tid, [])
        start_time = stop_times[0].get("departure_time") if stop_times else None
        end_time   = stop_times[-1].get("arrival_time")  if stop_times else None
        block_map[block_id].append({
            "trip_id": tid,
            "route_id": trip.get("route_id"),
            "route_short_name": route.get("route_short_name") if route else None,
            "headsign": trip.get("trip_headsign"),
            "start_time": start_time,
            "end_time": end_time,
            "_sort_key": first_departure_by_trip.get(tid, 0),
        })

    for block_id, course_list in block_map.items():
        course_list.sort(key=lambda c: c["_sort_key"])
        for idx, course in enumerate(course_list):
            course["order"] = idx
            del course["_sort_key"]

    return JsonResponse({"city": city, "feed": feed_name, "date": date_param, "blocks": block_map})


# ---------------------------------------------------------------------------
# Parsowanie protobuf
# ---------------------------------------------------------------------------
def _pb_trip_descriptor_to_dict(td):
    d = {}
    if td.HasField("trip_id") or td.trip_id:
        d["tripId"] = td.trip_id
    if td.HasField("route_id") or td.route_id:
        d["routeId"] = td.route_id
    if td.start_time:
        d["startTime"] = td.start_time
    if td.start_date:
        d["startDate"] = td.start_date
    if td.schedule_relationship:
        d["scheduleRelationship"] = td.DESCRIPTOR.fields_by_name[
            "schedule_relationship"].enum_type.values_by_number[td.schedule_relationship].name
    return d


def _pb_vehicle_descriptor_to_dict(vd):
    d = {}
    if vd.id:
        d["id"] = vd.id
    if vd.label:
        d["label"] = vd.label
    if vd.license_plate:
        d["licensePlate"] = vd.license_plate
    return d


def _pb_position_to_dict(pos):
    d = {"latitude": pos.latitude, "longitude": pos.longitude}
    if pos.bearing:
        d["bearing"] = pos.bearing
    if pos.speed:
        d["speed"] = pos.speed
    if pos.odometer:
        d["odometer"] = pos.odometer
    return d


def _pb_time_event_to_dict(te):
    d = {}
    if te.delay:
        d["delay"] = te.delay
    if te.time:
        d["time"] = te.time
    if te.uncertainty:
        d["uncertainty"] = te.uncertainty
    return d


def _pb_vehicle_position_to_dict(vp_entity):
    if not vp_entity.HasField("vehicle"):
        return None
    v = vp_entity.vehicle
    out = {}
    if v.HasField("trip"):
        out["trip"] = _pb_trip_descriptor_to_dict(v.trip)
    if v.HasField("vehicle"):
        out["vehicle"] = _pb_vehicle_descriptor_to_dict(v.vehicle)
    if v.HasField("position"):
        out["position"] = _pb_position_to_dict(v.position)
    if v.current_stop_sequence:
        out["currentStopSequence"] = v.current_stop_sequence
    if v.stop_id:
        out["stopId"] = v.stop_id
    if v.current_status:
        out["currentStatus"] = v.DESCRIPTOR.fields_by_name[
            "current_status"].enum_type.values_by_number[v.current_status].name
    if v.timestamp:
        out["timestamp"] = v.timestamp
    if v.congestion_level:
        out["congestionLevel"] = v.congestion_level
    if v.occupancy_status:
        out["occupancyStatus"] = v.occupancy_status
    return out


def _pb_trip_update_to_dict(tu_entity):
    if not tu_entity.HasField("trip_update"):
        return None
    tu = tu_entity.trip_update
    out = {}
    if tu.HasField("trip"):
        out["trip"] = _pb_trip_descriptor_to_dict(tu.trip)
    if tu.HasField("vehicle"):
        out["vehicle"] = _pb_vehicle_descriptor_to_dict(tu.vehicle)
    stop_time_updates = []
    for stu in tu.stop_time_update:
        s = {}
        if stu.stop_sequence:
            s["stopSequence"] = stu.stop_sequence
        if stu.stop_id:
            s["stopId"] = stu.stop_id
        if stu.HasField("arrival") and (stu.arrival.delay or stu.arrival.time):
            s["arrival"] = _pb_time_event_to_dict(stu.arrival)
        if stu.HasField("departure") and (stu.departure.delay or stu.departure.time):
            s["departure"] = _pb_time_event_to_dict(stu.departure)
        if stu.schedule_relationship:
            s["scheduleRelationship"] = stu.DESCRIPTOR.fields_by_name[
                "schedule_relationship"].enum_type.values_by_number[stu.schedule_relationship].name
        stop_time_updates.append(s)
    if stop_time_updates:
        out["stopTimeUpdate"] = stop_time_updates
    if tu.timestamp:
        out["timestamp"] = tu.timestamp
    if tu.delay:
        out["delay"] = tu.delay
    return out


def _pb_alert_to_dict(alert_entity):
    if not alert_entity.HasField("alert"):
        return None
    al = alert_entity.alert
    out = {}
    if al.active_period:
        out["activePeriod"] = [
            {k: v for k, v in [("start", p.start), ("end", p.end)] if v}
            for p in al.active_period
        ]
    if al.informed_entity:
        informed = []
        for ie in al.informed_entity:
            e = {}
            if ie.agency_id:
                e["agencyId"] = ie.agency_id
            if ie.route_id:
                e["routeId"] = ie.route_id
            if ie.route_type:
                e["routeType"] = ie.route_type
            if ie.HasField("trip"):
                e["trip"] = _pb_trip_descriptor_to_dict(ie.trip)
            if ie.stop_id:
                e["stopId"] = ie.stop_id
            informed.append(e)
        out["informedEntity"] = informed
    if al.cause:
        out["cause"] = al.DESCRIPTOR.fields_by_name[
            "cause"].enum_type.values_by_number[al.cause].name
    if al.effect:
        out["effect"] = al.DESCRIPTOR.fields_by_name[
            "effect"].enum_type.values_by_number[al.effect].name

    def _translated_string(ts):
        return {"translation": [
            {k: v for k, v in [("text", t.text), ("language", t.language)] if v}
            for t in ts.translation
        ]}

    if al.HasField("url"):
        out["url"] = _translated_string(al.url)
    if al.HasField("header_text"):
        out["headerText"] = _translated_string(al.header_text)
    if al.HasField("description_text"):
        out["descriptionText"] = _translated_string(al.description_text)
    return out


def _pb_feed_to_entities(feed_msg):
    header = feed_msg.header
    header_dict = {
        "gtfsRealtimeVersion": header.gtfs_realtime_version,
        "incrementality": header.DESCRIPTOR.fields_by_name[
            "incrementality"].enum_type.values_by_number[header.incrementality].name,
        "timestamp": header.timestamp,
    }
    entities = []
    for entity in feed_msg.entity:
        e = {"id": entity.id}
        if entity.HasField("vehicle"):
            vp = _pb_vehicle_position_to_dict(entity)
            if vp is not None:
                e["vehicle"] = vp
                entities.append(e)
        elif entity.HasField("trip_update"):
            tu = _pb_trip_update_to_dict(entity)
            if tu is not None:
                e["tripUpdate"] = tu
                entities.append(e)
        elif entity.HasField("alert"):
            al = _pb_alert_to_dict(entity)
            if al is not None:
                e["alert"] = al
                entities.append(e)
    return header_dict, entities


def _json_feed_to_entities(json_data: dict | list):
    if isinstance(json_data, list):
        return None, json_data
    header = json_data.get("header") or json_data.get("Header")
    header_dict = None
    if header:
        header_dict = {
            "gtfsRealtimeVersion": header.get("gtfsRealtimeVersion") or header.get("gtfs_realtime_version"),
            "incrementality": header.get("incrementality"),
            "timestamp": header.get("timestamp"),
        }
    entities = json_data.get("entity") or json_data.get("entities") or []
    return header_dict, entities


_vehicle_position_cache: dict[str, dict] = {}


def _compute_bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_lambda = math.radians(lon2 - lon1)
    x = math.sin(delta_lambda) * math.cos(phi2)
    y = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(delta_lambda)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def _build_tu_realtime_timestamps_for_trip(
    static_stops: list,
    trip_update_block: dict,
) -> list:
    if not static_stops or not trip_update_block:
        return []

    stu_list = (
        trip_update_block.get("stopTimeUpdate")
        or trip_update_block.get("stop_time_update")
        or []
    )

    tu_by_seq: dict[int, dict] = {}
    tu_by_sid: dict[str, dict] = {}
    for stu in stu_list:
        seq_raw = stu.get("stopSequence") if "stopSequence" in stu else stu.get("stop_sequence")
        sid_raw = stu.get("stopId") if "stopId" in stu else stu.get("stop_id")
        try:
            if seq_raw is not None:
                tu_by_seq[int(seq_raw)] = stu
        except Exception:
            pass
        if sid_raw is not None:
            tu_by_sid[str(sid_raw)] = stu

    today = datetime.now().date()
    result = []

    for st in sorted(static_stops, key=lambda x: int(x.get("stop_sequence", 0))):
        seq = int(st.get("stop_sequence", 0))
        sid = str(st.get("stop_id", ""))
        stu = tu_by_seq.get(seq) or tu_by_sid.get(sid)

        def _compute_real_ts(field: str) -> int | None:
            if not stu:
                return None
            ev = stu.get(field) or {}
            delay = ev.get("delay")
            ts = ev.get("time")
            if ts:
                try:
                    return int(ts)
                except Exception:
                    return None
            if delay is None:
                return None
            sched_str = st.get("arrival_time") if field == "arrival" else st.get("departure_time")
            if not sched_str:
                return None
            try:
                base_dt = parse_date_from_time(datetime.combine(today, datetime.min.time()), sched_str)
            except Exception:
                return None
            try:
                return int(base_dt.timestamp()) + int(delay)
            except Exception:
                return None

        arrival_real_ts = _compute_real_ts("arrival")
        departure_real_ts = _compute_real_ts("departure")

        result.append({
            "st": st,
            "seq": seq,
            "sid": sid,
            "arrival_real_ts": arrival_real_ts,
            "departure_real_ts": departure_real_ts,
        })

    return result


# ---------------------------------------------------------------------------
# Sprawdzanie czy kurs jeszcze nie rozpoczął kursowania
# ---------------------------------------------------------------------------
def _is_trip_not_yet_started(
    trip_id: str,
    indexes: dict,
    now_ts: int | None = None,
    threshold_seconds: int = 900,  # 15 minut
) -> bool:
    """
    Sprawdza czy kurs jeszcze nie rozpoczął kursowania – tzn. planowy odjazd
    z pierwszego przystanku (wg statycznego GTFS) jest oddalony o więcej niż
    `threshold_seconds` sekund w przyszłości względem `now_ts`.

    Dotyczy wyłącznie pojazdów estymowanych (bez VehiclePositions) – zapobiega
    wyświetlaniu „duchów" kursów, które dopiero za chwilę ruszają.

    Zwraca True jeśli do planowego odjazdu pozostało więcej niż threshold_seconds
    i kurs należy pominąć przy budowaniu syntetycznej encji vehicle.
    """
    if now_ts is None:
        now_ts = int(time.time())

    static_stops = indexes.get("stop_times_by_trip", {}).get(trip_id, [])
    if not static_stops:
        # Brak danych statycznych – nie możemy nic ocenić, przepuszczamy
        return False

    first_stop = static_stops[0]  # stop_times_by_trip są już posortowane rosnąco
    first_time_str = first_stop.get("departure_time") or first_stop.get("arrival_time")
    if not first_time_str:
        return False

    today = datetime.now().date()
    try:
        base_dt = datetime.combine(today, datetime.min.time())
        first_dt = parse_date_from_time(base_dt, first_time_str)
        first_ts = int(first_dt.timestamp())
    except Exception:
        return False

    return first_ts > now_ts + threshold_seconds


# ---------------------------------------------------------------------------
# Sprawdzanie czy kurs dobiegł końca
# ---------------------------------------------------------------------------
def _is_trip_finished(
    trip_id: str,
    trip_update_block: dict,
    indexes: dict,
    now_ts: int | None = None,
) -> bool:
    """
    Sprawdza czy kurs zakończył już kursowanie.

    Strategia (w kolejności priorytetów):
    1. Jeśli dostępne są timestampy z TripUpdate – porównujemy aktualny czas
       z realnym czasem przyjazdu/odjazdu ostatniego przystanku kursu.
    2. Fallback na statyczny rozkład – porównujemy aktualny czas z planowym
       czasem odjazdu ostatniego przystanku (z uwzględnieniem delay z TripUpdate).
    """
    if now_ts is None:
        now_ts = int(time.time())

    static_stops = indexes.get("stop_times_by_trip", {}).get(trip_id, [])
    if not static_stops:
        return False

    if trip_update_block:
        st_real = _build_tu_realtime_timestamps_for_trip(static_stops, trip_update_block)
        if st_real:
            last = st_real[-1]
            last_ts = last.get("arrival_real_ts") or last.get("departure_real_ts")
            if last_ts is not None:
                return now_ts > last_ts

    last_stop = static_stops[-1]
    last_time_str = last_stop.get("arrival_time") or last_stop.get("departure_time")
    if not last_time_str:
        return False

    static_delay = 0
    if trip_update_block:
        stu_list = (
            trip_update_block.get("stopTimeUpdate")
            or trip_update_block.get("stop_time_update")
            or []
        )
        if stu_list:
            last_stu = stu_list[-1]
            arr = last_stu.get("arrival") or {}
            dep = last_stu.get("departure") or {}
            raw_delay = dep.get("delay") if dep.get("delay") is not None else arr.get("delay")
            if raw_delay is not None:
                try:
                    static_delay = int(raw_delay)
                except Exception:
                    static_delay = 0

    today = datetime.now().date()
    try:
        base_dt = parse_date_from_time(datetime.combine(today, datetime.min.time()), last_time_str)
        last_ts_static = int(base_dt.timestamp()) + static_delay
    except Exception:
        return False

    return now_ts > last_ts_static


def _estimate_delay_from_position(
    est: dict,
    static_stops: list,
    now_ts: int,
) -> int | None:
    progress = est.get("progress")
    stopA_seq = est.get("stopA_seq")
    stopB_seq = est.get("stopB_seq")

    if progress is None or stopA_seq is None or stopB_seq is None:
        return None

    st_by_seq: dict[int, dict] = {int(st.get("stop_sequence", -1)): st for st in static_stops}
    st_A = st_by_seq.get(stopA_seq)
    st_B = st_by_seq.get(stopB_seq)

    if st_A is None or st_B is None:
        return None

    today = datetime.now().date()
    base_dt = datetime.combine(today, datetime.min.time())

    def _sched_ts(stop_time_row: dict, field: str) -> int | None:
        t_str = stop_time_row.get(field)
        if not t_str:
            return None
        try:
            return int(parse_date_from_time(base_dt, t_str).timestamp())
        except Exception:
            return None

    sched_dep_A = _sched_ts(st_A, "departure_time") or _sched_ts(st_A, "arrival_time")
    sched_arr_B = _sched_ts(st_B, "arrival_time") or _sched_ts(st_B, "departure_time")

    if sched_dep_A is None:
        return None

    if stopA_seq == stopB_seq or sched_arr_B is None or sched_arr_B <= sched_dep_A:
        return now_ts - sched_dep_A

    expected_ts = sched_dep_A + progress * (sched_arr_B - sched_dep_A)
    return int(round(now_ts - expected_ts))


def _estimate_position_from_trip_update(
    feed_name: str,
    trip_id: str,
    trip_update_block: dict,
    indexes: dict,
    now_ts: int | None = None,
) -> dict | None:
    static_stops = indexes.get("stop_times_by_trip", {}).get(trip_id, [])
    if not static_stops:
        return None

    stops_by_id = indexes.get("stops_by_id", {})
    trips_by_id = indexes.get("trips_by_id", {})

    trip_obj = trips_by_id.get(trip_id)
    shape_id = trip_obj.get("shape_id") if trip_obj else None
    shape_points = indexes.get("shapes_by_id", {}).get(shape_id, []) if shape_id else []

    st_real = _build_tu_realtime_timestamps_for_trip(static_stops, trip_update_block)
    if not st_real:
        return None

    if now_ts is None:
        now_ts = int(time.time())

    idxA = None
    for i, item in enumerate(st_real):
        dep_ts = item.get("departure_real_ts") or item.get("arrival_real_ts")
        if dep_ts is None:
            continue
        if dep_ts <= now_ts:
            idxA = i
        else:
            break

    if idxA is None:
        if len(st_real) < 2:
            return None
        stopA = st_real[0]
        stopB = st_real[1]
        progress = 0.0
    elif idxA >= len(st_real) - 1:
        stopA = st_real[-1]
        stopB = st_real[-1]
        progress = 1.0
    else:
        stopA = st_real[idxA]
        stopB = st_real[idxA + 1]
        depA = stopA.get("departure_real_ts") or stopA.get("arrival_real_ts")
        arrB = stopB.get("arrival_real_ts") or stopB.get("departure_real_ts")
        if depA is None or arrB is None or arrB <= depA:
            progress = 0.0
        else:
            progress = (now_ts - depA) / (arrB - depA)
            if now_ts < depA:
                progress = 0.0
        progress = max(0.0, min(float(progress), 1.0))

    stopA_st = stopA["st"]
    stopB_st = stopB["st"]
    stopA_id = str(stopA_st.get("stop_id", ""))
    stopB_id = str(stopB_st.get("stop_id", ""))

    distA = stopA_st.get("shape_dist_traveled")
    distB = stopB_st.get("shape_dist_traveled")
    lat = lon = None

    if distA is not None and distB is not None:
        try:
            target_dist = float(distA) + progress * (float(distB) - float(distA))
        except Exception:
            target_dist = None
        if target_dist is not None and shape_points:
            pos = interpolate_on_shape(shape_points, target_dist)
            if pos is not None:
                lat, lon = pos

    if lat is None or lon is None:
        infoA = stops_by_id.get(stopA_id)
        infoB = stops_by_id.get(stopB_id)
        try:
            if infoA and infoB:
                lat = float(infoA["stop_lat"]) + progress * (float(infoB["stop_lat"]) - float(infoA["stop_lat"]))
                lon = float(infoA["stop_lon"]) + progress * (float(infoB["stop_lon"]) - float(infoA["stop_lon"]))
            elif infoA:
                lat = float(infoA["stop_lat"])
                lon = float(infoA["stop_lon"])
            elif infoB:
                lat = float(infoB["stop_lat"])
                lon = float(infoB["stop_lon"])
        except Exception:
            lat = lon = None

    if lat is None or lon is None:
        return None

    return {
        "trip_id": trip_id,
        "lat": lat,
        "lon": lon,
        "progress": float(progress),
        "segment_stopA": stopA_id,
        "segment_stopB": stopB_id,
        "stopA_seq": int(stopA.get("seq", 0)),
        "stopB_seq": int(stopB.get("seq", 0)),
    }


def interpolate_on_shape(shape_points: list, target_dist: float) -> tuple[float, float] | None:
    if not shape_points:
        return None

    pts = sorted(shape_points, key=lambda x: int(x.get("shape_pt_sequence", 0)))
    try:
        dists = [float(p.get("shape_dist_traveled", 0.0)) for p in pts]
    except Exception:
        return None

    if not dists:
        return None
    if target_dist <= dists[0]:
        return float(pts[0]["shape_pt_lat"]), float(pts[0]["shape_pt_lon"])
    if target_dist >= dists[-1]:
        return float(pts[-1]["shape_pt_lat"]), float(pts[-1]["shape_pt_lon"])

    for i in range(len(pts) - 1):
        d0, d1 = dists[i], dists[i + 1]
        if d0 <= target_dist <= d1:
            lat0, lon0 = float(pts[i]["shape_pt_lat"]), float(pts[i]["shape_pt_lon"])
            lat1, lon1 = float(pts[i + 1]["shape_pt_lat"]), float(pts[i + 1]["shape_pt_lon"])
            if d1 == d0:
                return (lat0, lon0) if abs(target_dist - d0) <= abs(target_dist - d1) else (lat1, lon1)
            ratio = (target_dist - d0) / (d1 - d0)
            return float(lat0 + ratio * (lat1 - lat0)), float(lon0 + ratio * (lon1 - lon0))

    return None


def build_synthetic_vehicle_entity_from_tu(
    feed_name: str,
    trip_update_entity: dict,
    indexes: dict,
    now_ts: int | None = None,
) -> dict | None:
    """
    Buduje syntetyczną encję vehicle na podstawie TripUpdate i statycznego GTFS.

    Zwraca None gdy:
    - kurs dobiegł już końca (_is_trip_finished),
    - planowy odjazd z pierwszego przystanku jest więcej niż 15 minut
      w przyszłości (_is_trip_not_yet_started),
    - nie można wyestymować pozycji.
    """
    tu = trip_update_entity.get("tripUpdate") or trip_update_entity.get("trip_update")
    if not tu:
        return None

    trip_info = tu.get("trip", {})
    trip_id = trip_info.get("tripId") or trip_info.get("trip_id")
    if not trip_id:
        return None

    route_id = trip_info.get("routeId") or trip_info.get("route_id")
    if now_ts is None:
        now_ts = int(time.time())

    # Pomiń kurs jeśli już zakończył kursowanie
    if _is_trip_finished(trip_id, tu, indexes, now_ts=now_ts):
        return None

    # Pomiń kurs jeśli planowy odjazd z pierwszego przystanku jest
    # więcej niż 15 minut w przyszłości – kurs jeszcze nie ruszył
    if _is_trip_not_yet_started(trip_id, indexes, now_ts=now_ts):
        return None

    est = _estimate_position_from_trip_update(feed_name, trip_id, tu, indexes, now_ts=now_ts)
    if not est:
        return None

    synthetic_vehicle_id = f"EST-{trip_id}"

    vehicle_block = {
        "trip": {"tripId": trip_id},
        "vehicle": {"id": synthetic_vehicle_id},
        "position": {"latitude": est["lat"], "longitude": est["lon"]},
        "timestamp": now_ts,
    }
    if route_id:
        vehicle_block["trip"]["routeId"] = route_id

    stopB_seq = est.get("stopB_seq")
    if stopB_seq is not None:
        vehicle_block["currentStopSequence"] = stopB_seq

    return {
        "id": synthetic_vehicle_id,
        "positionType": "estimated",
        "vehicle": vehicle_block,
    }


@require_GET
def get_parsed_realtime_for_feed(request):
    feed_name = request.GET.get("feed_name")
    if not feed_name:
        return error("Missing parameter: feed_name")

    feed_obj = GTFSFeed.objects.filter(name=feed_name).first()
    if not feed_obj:
        return error(f"Feed '{feed_name}' not found", 404)

    has_any_rt = any([
        feed_obj.vehicle_positions_url,
        feed_obj.trip_updates_url,
        feed_obj.service_alerts_url,
    ])
    if not has_any_rt:
        return JsonResponse({
            "feed_name": feed_name,
            "realtime_available": False,
            "message": "Ten feed nie ma skonfigurowanych żadnych linków realtime.",
        })

    url_to_keys = {}
    for key, url in [
        ("vehicle_positions", feed_obj.vehicle_positions_url),
        ("trip_updates", feed_obj.trip_updates_url),
        ("alerts", feed_obj.service_alerts_url),
    ]:
        if url:
            url_to_keys.setdefault(url, []).append(key)

    combined_header = None
    combined_entities = []
    fetch_errors = []

    def _fetch_and_parse_url(url: str):
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            content_type = resp.headers.get("Content-Type", "")
            if url.endswith(".json") or "application/json" in content_type:
                data = resp.json()
                hdr, entities = _json_feed_to_entities(data)
            else:
                feed_pb = gtfs_realtime_pb2.FeedMessage()
                feed_pb.ParseFromString(resp.content)
                hdr, entities = _pb_feed_to_entities(feed_pb)
            return url, hdr, entities, None
        except Exception as exc:
            return url, None, [], str(exc)

    with ThreadPoolExecutor(max_workers=len(url_to_keys)) as executor:
        futures = {executor.submit(_fetch_and_parse_url, url): url for url in url_to_keys}
        for future in as_completed(futures):
            url, hdr, entities, err = future.result()
            if err:
                fetch_errors.append(f"Nie udało się pobrać {url}: {err}")
            else:
                if hdr and combined_header is None:
                    combined_header = hdr
                combined_entities.extend(entities)

    feed_ts = (combined_header or {}).get("timestamp") or 0

    indexes = _ensure_feed_indexes(feed_name)
    if indexes:
        trips_with_vehicle: set[str] = set()
        tu_entities_by_trip: dict[str, list] = {}
        for entity in combined_entities:
            vehicle_block = entity.get("vehicle")
            if vehicle_block:
                trip_block = vehicle_block.get("trip", {})
                tid = trip_block.get("tripId") or trip_block.get("trip_id")
                if tid:
                    trips_with_vehicle.add(tid)
            tu_block = entity.get("tripUpdate") or entity.get("trip_update")
            if tu_block:
                trip_block = tu_block.get("trip", {})
                tid = trip_block.get("tripId") or trip_block.get("trip_id")
                if tid:
                    tu_entities_by_trip.setdefault(tid, []).append(entity)

        for tid, entities_for_trip in tu_entities_by_trip.items():
            if tid in trips_with_vehicle:
                continue
            chosen_entity = entities_for_trip[0]
            synthetic = build_synthetic_vehicle_entity_from_tu(
                feed_name,
                chosen_entity,
                indexes,
                now_ts=feed_ts or int(time.time()),
            )
            if synthetic:
                combined_entities.append(synthetic)

    for entity in combined_entities:
        vehicle_block = entity.get("vehicle")
        if not vehicle_block:
            continue
        if "positionType" not in entity:
            entity["positionType"] = "real"

        position = vehicle_block.get("position")
        if not position:
            continue
        lat = position.get("latitude")
        lon = position.get("longitude")
        if lat is None or lon is None:
            continue
        veh_info = vehicle_block.get("vehicle") or {}
        vehicle_id = veh_info.get("id") or entity.get("id") or "unknown"
        cache_key = f"{feed_name}:{vehicle_id}"
        prev = _vehicle_position_cache.get(cache_key)
        if prev and (prev["lat"] != lat or prev["lon"] != lon):
            bearing = _compute_bearing(prev["lat"], prev["lon"], lat, lon)
            position["calculatedBearing"] = round(bearing, 1)
            if not position.get("speed") and prev.get("ts") and feed_ts and feed_ts > prev["ts"]:
                dt = feed_ts - prev["ts"]
                dist_km = haversine(prev["lat"], prev["lon"], lat, lon)
                speed_ms = (dist_km * 1000) / dt
                position["calculatedSpeed"] = round(speed_ms, 2)
            else:
                position["calculatedSpeed"] = None
        else:
            position["calculatedBearing"] = None
            position["calculatedSpeed"] = None
        _vehicle_position_cache[cache_key] = {"lat": lat, "lon": lon, "ts": feed_ts}

    response = {
        "feed_name": feed_name,
        "realtime_available": True,
        "header": combined_header or {},
        "entity": combined_entities,
    }
    if fetch_errors:
        response["fetch_errors"] = fetch_errors
    return JsonResponse(response)


# ---------------------------------------------------------------------------
# Usuwamy starą _build_service_dates_map, bo zastąpiliśmy ją get_service_dates_map
# ---------------------------------------------------------------------------
# def _build_service_dates_map(...): ...