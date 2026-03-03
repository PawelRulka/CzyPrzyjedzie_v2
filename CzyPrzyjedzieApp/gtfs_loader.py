# CzyPrzyjedzieApp/gtfs_loader.py
import csv
import io
import zipfile
import requests
from datetime import datetime

from .models import GTFSFeed

# Globalny cache GTFS w pamięci (mutowalny słownik)
GTFS_DATA = {}
GTFS_LAST_LOAD = None


def load_all_gtfs():
    """
    Ładuje wszystkie aktywne feedy GTFS.
    WAŻNE: NIE przypisujemy nowego obiektu do GTFS_DATA (to by zerowało referencje
    w innych modułach). Zamiast tego czyścimy i aktualizujemy istniejący słownik.
    """
    global GTFS_LAST_LOAD

    # Czyścimy istniejący słownik, żeby inne moduły które mają referencję też zobaczyły zmiany
    GTFS_DATA.clear()

    feeds = GTFSFeed.objects.filter(is_active=True)

    for feed in feeds:
        try:
            GTFS_DATA[feed.name] = load_single_feed(feed)
        except Exception as e:
            # Nie przerywamy ładowania wszystkich feedów — można dodać logging
            # print(f"Nie udało się załadować feedu {feed.name}: {e}")
            continue

    GTFS_LAST_LOAD = datetime.now()


def load_single_feed(feed: GTFSFeed):
    """
    Ładuje pojedynczy plik GTFS (zip) i parsuje pliki CSV:
    stops.txt, routes.txt, trips.txt, stop_times.txt, shapes.txt, calendar.txt, calendar_dates.txt
    Zwraca słownik z listami rekordów (dict).
    """
    response = requests.get(feed.static_url, timeout=30)
    response.raise_for_status()

    zip_bytes = io.BytesIO(response.content)

    with zipfile.ZipFile(zip_bytes) as zf:
        data = {
            "stops": parse_csv(zf, "stops.txt"),
            "routes": parse_csv(zf, "routes.txt"),
            "trips": parse_csv(zf, "trips.txt"),
            "stop_times": parse_csv(zf, "stop_times.txt"),
            "shapes": parse_csv(zf, "shapes.txt"),
            "calendar": parse_csv(zf, "calendar.txt"),
            "calendar_dates": parse_csv(zf, "calendar_dates.txt"),
        }

    return data


def parse_csv(zip_file, filename):
    """
    Zwraca listę dictów z pliku CSV w zipie, lub [] jeśli plik nie istnieje.
    Używamy encodingu utf-8-sig żeby być odpornym na BOM.
    """
    if filename not in zip_file.namelist():
        return []

    with zip_file.open(filename) as f:
        text = io.TextIOWrapper(f, encoding="utf-8-sig")
        reader = csv.DictReader(text)
        return list(reader)


def ensure_gtfs_loaded():
    """
    Jeżeli cache jest pusty — ładujemy wszystkie feedy.
    """
    if not GTFS_DATA:
        load_all_gtfs()