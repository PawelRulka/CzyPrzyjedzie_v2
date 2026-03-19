#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import io
import os
import zipfile
import requests
from datetime import datetime, timedelta
from http.server import HTTPServer, SimpleHTTPRequestHandler
import threading
import time

# ================== KONFIGURACJA ==================
GTFS_URL = "http://cdn.zbiorkom.live/gtfs/bialystok.zip"
OUTPUT_ZIP = "bialystok_modified.zip"
HOST = "localhost"
PORT = 8070

# Definicje merge dla block_id
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
        "service_id": "P",
        "brigades":   ["201-03", "202-02", "202-03", "202-01"],
        "block_id":   "201-03+202-02+202-03+202-01",
    },
    {
        "service_id": "P",
        "brigades":   ["201-01", "200-01", "201-02", "200-02"],
        "block_id":   "201-01+200-01+201-02+200-02",
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

# Definicje kolorów dla tras
ROUTE_COLORS = [
    {"lines": "1-6", "route_color": "6DB532", "route_text_color": "FFFFFF"},
    {"lines": "8-9", "route_color": "6DB532", "route_text_color": "FFFFFF"},
    {"lines": "11-21", "route_color": "6DB532", "route_text_color": "FFFFFF"},
    {"lines": "23-30", "route_color": "6DB532", "route_text_color": "FFFFFF"},
    {"lines": "50", "route_color": "6DB532", "route_text_color": "FFFFFF"},
    {"lines": ["7", "10", "22"], "route_color": "225AC9", "route_text_color": "FFFFFF"},
    {"lines": ["200", "201", "202"], "route_color": "BE080A", "route_text_color": "FFFFFF"},
    {"lines": "100-142", "route_color": "225AC9", "route_text_color": "FFFFFF"},
    {"lines": ["N1", "N2", "N3", "N4", "N5", "N6"], "route_color": "334455", "route_text_color": "FFFFFF"},
    {"lines": ["J", "J1", "J2", "J3", "J4", "J5", "J6"], "route_color": "D83E46", "route_text_color": "FFFFFF"},
]


# ================== FUNKCJE POMOCNICZE ==================
def parse_range(range_str):
    """Konwertuje string 'a-b' lub pojedynczą liczbę na zbiór stringów (np. '1-6' -> {'1','2','3','4','5','6'})."""
    if '-' in range_str:
        start, end = range_str.split('-')
        # Zakładamy, że start i end to liczby całkowite (np. '1' i '6')
        return {str(i) for i in range(int(start), int(end) + 1)}
    else:
        return {range_str}


def build_route_color_map(route_colors):
    """
    Tworzy słownik mapujący route_id (string) na krotkę (route_color, route_text_color).
    Obsługuje zarówno pojedyncze linie, listy, jak i zakresy.
    """
    color_map = {}
    for item in route_colors:
        lines = item["lines"]
        color = item["route_color"]
        text_color = item["route_text_color"]

        if isinstance(lines, list):
            # Lista konkretnych identyfikatorów
            for line in lines:
                color_map[line] = (color, text_color)
        elif isinstance(lines, str):
            # Pojedynczy identyfikator lub zakres
            if '-' in lines:
                for line in parse_range(lines):
                    color_map[line] = (color, text_color)
            else:
                color_map[lines] = (color, text_color)
        else:
            raise ValueError(f"Nieobsługiwany typ 'lines': {type(lines)}")
    return color_map


def build_brigade_map(merges):
    """
    Tworzy mapę (service_prefix, brigade) -> nowy block_id.
    service_prefix to 'P', 'R', 'S' – dla tych prefixów sprawdzamy dopasowanie service_id.
    """
    brigade_map = {}
    for merge in merges:
        prefix = merge["service_id"]
        for brigade in merge["brigades"]:
            brigade_map[(prefix, brigade)] = merge["block_id"]
    return brigade_map


def matches_service_id(service_id, prefix):
    """
    Sprawdza, czy service_id pasuje do prefixu (np. 'P' pasuje do 'P', 'P-0', 'P-1' itd.)
    """
    if service_id == prefix:
        return True
    if service_id.startswith(prefix + "-"):
        return True
    return False


def modify_trips(trips_content, brigade_map):
    """
    Modyfikuje zawartość trips.txt:
      - zmienia nazwę kolumny 'brigade' na 'block_id'
      - podmienia wartości block_id zgodnie z brigade_map
    Zwraca nową zawartość jako string.
    """
    input_file = io.StringIO(trips_content)
    output_file = io.StringIO()

    reader = csv.DictReader(input_file)
    if 'brigade' not in reader.fieldnames:
        raise ValueError("Plik trips.txt nie zawiera kolumny 'brigade'.")

    # Zmieniamy nazwę kolumny w fieldnames
    new_fieldnames = [('block_id' if f == 'brigade' else f) for f in reader.fieldnames]
    writer = csv.DictWriter(output_file, fieldnames=new_fieldnames, quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()

    for row in reader:
        # Przygotowujemy nowy wiersz z przemianowaną kolumną
        new_row = {}
        for key, value in row.items():
            new_key = 'block_id' if key == 'brigade' else key
            new_row[new_key] = value

        # Sprawdzamy, czy trzeba podmienić block_id
        service_id = row['service_id']
        brigade = row['brigade']  # oryginalna wartość

        # Szukamy pasującego prefixu
        for prefix in {'P', 'R', 'S'}:
            if matches_service_id(service_id, prefix):
                key = (prefix, brigade)
                if key in brigade_map:
                    new_row['block_id'] = brigade_map[key]
                    break  # zakładamy, że tylko jeden prefix może pasować

        writer.writerow(new_row)

    return output_file.getvalue()


def modify_routes(routes_content, color_map):
    """
    Modyfikuje zawartość routes.txt:
      - ustawia route_color i route_text_color zgodnie z color_map dla znanych route_id
      - jeśli kolumna route_text_color nie istnieje, zostanie dodana
    Zwraca nową zawartość jako string.
    """
    input_file = io.StringIO(routes_content)
    output_file = io.StringIO()

    reader = csv.DictReader(input_file)

    # Sprawdzamy, jakie kolumny są dostępne
    original_fieldnames = reader.fieldnames.copy()

    # Upewniamy się, że mamy obie kolumny kolorów
    new_fieldnames = original_fieldnames.copy()
    needs_text_color = 'route_text_color' not in new_fieldnames

    if needs_text_color:
        # Jeśli nie ma route_text_color, dodajemy ją po route_color (lub na końcu)
        if 'route_color' in new_fieldnames:
            idx = new_fieldnames.index('route_color')
            new_fieldnames.insert(idx + 1, 'route_text_color')
        else:
            new_fieldnames.append('route_text_color')

    writer = csv.DictWriter(output_file, fieldnames=new_fieldnames, quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()

    for row in reader:
        # Tworzymy nowy wiersz z wszystkimi oryginalnymi kolumnami
        new_row = row.copy()

        route_id = row['route_id']
        if route_id in color_map:
            new_row['route_color'] = color_map[route_id][0]
            # Jeśli potrzeba, dodajemy route_text_color
            if needs_text_color:
                new_row['route_text_color'] = color_map[route_id][1]
            else:
                # Jeśli kolumna istnieje, aktualizujemy ją
                new_row['route_text_color'] = color_map[route_id][1]

        writer.writerow(new_row)

    return output_file.getvalue()


def generate_feed_info(now):
    """
    Generuje zawartość feed_info.txt z dynamicznymi datami.
    now: datetime object (moment generowania)
    """
    feed_version = now.strftime("%Y%m%d%H%M%S")
    feed_start_date = now.strftime("%Y%m%d")
    feed_end_date = (now + timedelta(days=90)).strftime("%Y%m%d")

    header = "feed_lang,feed_version,feed_start_date,feed_end_date,feed_publisher_url,feed_contact_url,feed_publisher_name"
    row = f"pl,{feed_version},{feed_start_date},{feed_end_date},https://github.com/pawlinski07/pawlinski-gtfs,https://github.com/pawlinski07/pawlinski-gtfs/issues,\"Unofficial GTFS of Białostocka Komunikacja Miejska, generated by pawlinski07\""

    return header + "\n" + row


def download_gtfs(url):
    """Pobiera plik GTFS z podanego URL i zwraca jego zawartość jako bytes."""
    print(f"Pobieranie GTFS z {url}...")
    response = requests.get(url)
    response.raise_for_status()
    return response.content


def process_gtfs(zip_bytes):
    """
    Przetwarza archiwum ZIP:
      - wypakowuje pliki
      - modyfikuje trips.txt, routes.txt, feed_info.txt
      - tworzy nowe archiwum ZIP z wszystkimi plikami (zmodyfikowanymi i niezmienionymi)
    Zwraca bytes nowego archiwum.
    """
    # Przygotowanie map
    color_map = build_route_color_map(ROUTE_COLORS)
    brigade_map = build_brigade_map(BLOCK_ID_MERGES)

    # Otwieramy oryginalne archiwum
    with zipfile.ZipFile(io.BytesIO(zip_bytes), 'r') as in_zip:
        # Tworzymy nowe archiwum w pamięci
        out_zip_bytes = io.BytesIO()
        with zipfile.ZipFile(out_zip_bytes, 'w', compression=zipfile.ZIP_DEFLATED) as out_zip:
            for file_name in in_zip.namelist():
                # Pomijamy katalogi (jeśli są)
                if file_name.endswith('/'):
                    continue

                # Odczytujemy oryginalną zawartość
                content = in_zip.read(file_name).decode('utf-8')

                # Modyfikujemy odpowiednie pliki
                if file_name == 'trips.txt':
                    print("Modyfikacja trips.txt...")
                    modified = modify_trips(content, brigade_map)
                elif file_name == 'routes.txt':
                    print("Modyfikacja routes.txt...")
                    modified = modify_routes(content, color_map)
                elif file_name == 'feed_info.txt':
                    print("Generowanie nowego feed_info.txt...")
                    modified = generate_feed_info(datetime.now())
                else:
                    modified = content  # bez zmian

                # Zapisujemy do nowego archiwum
                out_zip.writestr(file_name, modified.encode('utf-8'))

        return out_zip_bytes.getvalue()


def start_server(host, port, filename):
    """
    Uruchamia prosty serwer HTTP w tle, który serwuje plik filename.
    """

    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory='.', **kwargs)

        def do_GET(self):
            if self.path == '/' or self.path == f'/{filename}':
                self.path = f'/{filename}'
                return super().do_GET()
            else:
                self.send_response(404)
                self.end_headers()

    server = HTTPServer((host, port), Handler)
    print(f"Serwer uruchomiony na http://{host}:{port}/{filename}")
    server.serve_forever()


# ================== GŁÓWNY PROGRAM ==================
def main():
    # Pobieramy GTFS
    try:
        gtfs_bytes = download_gtfs(GTFS_URL)
    except Exception as e:
        print(f"Błąd pobierania GTFS: {e}")
        return

    # Przetwarzamy
    try:
        modified_gtfs_bytes = process_gtfs(gtfs_bytes)
    except Exception as e:
        print(f"Błąd przetwarzania GTFS: {e}")
        return

    # Zapisujemy zmodyfikowane archiwum na dysku
    with open(OUTPUT_ZIP, 'wb') as f:
        f.write(modified_gtfs_bytes)
    print(f"Zmodyfikowany GTFS zapisany jako {OUTPUT_ZIP}")

    # Uruchamiamy serwer w tle
    server_thread = threading.Thread(target=start_server, args=(HOST, PORT, OUTPUT_ZIP), daemon=True)
    server_thread.start()

    # Podtrzymujemy działanie skryptu (serwer działa w tle)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nZatrzymywanie serwera...")


if __name__ == "__main__":
    main()