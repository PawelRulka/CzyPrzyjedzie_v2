# Specyfikacja aplikacji

CzyPrzyjedzie to aplikacja która ma służyć do śledzenia pojazdów komunikacji miejskiej w czasie rzeczywistym.

## 1. Funkcjonalności techniczne

- Aplikacja może obsługiwać wiele miast naraz, użytkownik wybiera dla którego chce zobaczyć komunikację miejską na stronie głównej
- Po wejściu na stronę danego miasta pokazuje się mapa, na której są przystanki oraz pojazdy
- Użytkownik może zobaczyć teorytyczne, oraz rzeczywiste odjazdy z każdego przystanku
- Użytkownik może zobaczyć trasę każdego pojazdu na mapie
- Użytkownik może zobaczyć szczegóły trasy każdej linii w danym mieście, oraz wszystkie brygady¹ które obsługują tą linię

¹ - Brygada - zaplanowany ciąg kursów wykonywanych przez ten sam pojazd

Aplikacja w celu pokazania tych danych użytkownikowi korzysta z własnego API, które konwertuje dane w formacie GTFS (format danych dla transportu publicznego - https://gtfs.org/) na pliki JSON.

## 2. Wygląd aplikacji

- Aplikacja jest stroną WWW z widokiem pełnoekranowej mapy oraz nakładanymi panelami UI (układ „mapa + overlay”).
- Strona główna (wybór miasta):
  - mapa w tle,
  - centralna karta z nazwą aplikacji, krótkim opisem oraz listą miast jako przyciski,
  - każda pozycja miasta może zawierać ikonę kraju oraz opis.
- Widok miasta (mapa + dane):
  - mapa zajmuje całe okno,
  - na mapie wyświetlane są:
    - przystanki jako markery,
    - pojazdy jako markery z etykietą (np. numer boczny) oraz strzałką/kierunkiem jazdy,
  - po kliknięciu w przystanek/pojazd użytkownik otrzymuje szczegóły (np. odjazdy, kurs, opóźnienie) w panelach/okienkach nałożonych na mapę.
- Styl graficzny:
  - ciemny motyw z efektem „glass” (półprzezroczyste panele, blur),
  - nowoczesne, zaokrąglone komponenty, wyraźne cienie dla czytelności na tle mapy,
  - ikony wektorowe (Lucide) oraz fonty web (Roboto), z fallbackiem do systemowych.
- Mapa:
  - silnik mapy: MapLibre GL,
  - źródło kafelków/stylu: MapTiler (z atrybucją MapTiler + OpenStreetMap).

## 3. Wymagania techniczne

### Backend (serwer aplikacji)

- Framework: Django (konfiguracja projektu wskazuje na Django 5.2.11).
- Baza danych (domyślnie): SQLite (`db.sqlite3`).
- Aplikacja serwuje:
  - HTML (szablony Django w katalogu `templates/`),
  - statyczne zasoby (katalog `static/`, m.in. `static/css/main.css`).

### Zależności Pythona (wynikające z kodu)

- `django`
- `django-cors-headers` (middleware CORS skonfigurowany w `settings.py`)
- `requests` (pobieranie GTFS static i/lub danych realtime z URL-i)
- `gtfs-realtime-bindings` (moduł `google.transit.gtfs_realtime_pb2` do obsługi Protobuf GTFS-RT)
- Dodatkowe skrypty narzędziowe w repozytorium korzystają m.in. z `aiohttp` (generator/serwer OnTimeToGTFS).

### Frontend (przeglądarka)

- Brak osobnego bundlera (brak `package.json`) — UI to HTML/CSS/JS w szablonach.
- Zewnętrzne biblioteki wczytywane z CDN:
  - MapLibre GL (`unpkg.com`),
  - Lucide (`unpkg.com`),
  - Google Fonts (Roboto).
- Wymagane połączenie z internetem do pobrania stylu mapy (MapTiler) oraz bibliotek z CDN.

### Dane i integracje

- Aplikacja operuje na danych w formacie **GTFS Static** (zip z plikami `.txt`) oraz opcjonalnie **GTFS Realtime** (protobuf `.pb` lub JSON).
- Backend posiada endpointy API (sekcja poniżej), które łączą dane statyczne z realtime oraz zwracają ujednolicone JSON-y dla UI.
- Realtime może pochodzić z różnych źródeł (TripUpdates, VehiclePositions); przy braku VehiclePositions serwer potrafi estymować pozycje pojazdów na podstawie danych statycznych i TripUpdates.

# Dokumentacja API

Dokumentacja endpointów HTTP oraz funkcji pomocniczych modułu `api.py`. Wszystkie endpointy przyjmują wyłącznie żądania **GET** i zwracają odpowiedź w formacie **JSON**.

---

## Spis treści

- [Endpointy](#endpointy)
  - [getStopsForCity.json](#getstopsforcityjson)
  - [getRoutesForCity.json](#getroutesforcityjson)
  - [getScheduleForStop.json](#getscheduleforstopjson)
  - [getTripDetails.json](#gettripdetailsjson)
  - [getLineBrigadeDelayForTrip.json](#getlinebrigadedelayfortripjson)
  - [getTripByVehicle.json](#gettripbyvehiclejson)
  - [getRouteDetails.json](#getroutedetailsjson)
  - [getBlockScheduleForRoute.json](#getblockscheduleforroutejson)
  - [getTheoriticalBlockDetails.json](#gettheoriticalblockdetailsjson)
  - [getBlocksForFeedAndDate.json](#getblocksforfeedanddatejson)
  - [getParsedRealtimeForFeed.json](#getparsedrealtimeforfeedjson)
- [Typy danych i słownik pól](#typy-danych-i-słownik-pól)
- [Kody błędów](#kody-błędów)
- [Funkcje wewnętrzne](#funkcje-wewnętrzne)

---

## Endpointy

### getStopsForCity.json

Zwraca listę wszystkich przystanków dla danego miasta wraz z przypisanymi liniami.

**Parametry zapytania**

| Parametr | Wymagany | Opis |
|----------|----------|------|
| `name` | ✅ | Nazwa miasta (np. `Gdańsk`) |

**Przykładowe żądanie**

```
GET /api/getStopsForCity.json?name=Gdańsk
```

**Odpowiedź**

```json
{
  "city": "Gdańsk",
  "count": 312,
  "stops": [
    {
      "stop_id": "123",
      "stop_code": "GD01",
      "stop_name": "Dworzec Główny",
      "lat": 54.35615,
      "lon": 18.64387,
      "routes": [
        {
          "feed": "ZTM_GDA",
          "route_id": "1",
          "route_short_name": "1"
        }
      ]
    }
  ]
}
```

> Przystanki o identycznych współrzędnych (zaokrąglonych do 5 miejsc po przecinku) są traktowane jako jeden przystanek. Pole `routes` zawiera deduplikowaną listę linii obsługujących dany przystanek.

---

### getRoutesForCity.json

Zwraca listę wszystkich linii komunikacyjnych dla danego miasta.

**Parametry zapytania**

| Parametr | Wymagany | Opis |
|----------|----------|------|
| `name` | ✅ | Nazwa miasta |

**Przykładowe żądanie**

```
GET /api/getRoutesForCity.json?name=Gdańsk
```

**Odpowiedź**

```json
{
  "city": "Gdańsk",
  "routes": [
    {
      "feed": "ZTM_GDA",
      "route_id": "1",
      "route_short_name": "1",
      "color": "FF0000",
      "text_color": "FFFFFF"
    }
  ]
}
```

---

### getScheduleForStop.json

Zwraca rozkład jazdy dla konkretnego przystanku, wzbogacony o dane **realtime**.

**Parametry zapytania**

| Parametr | Wymagany | Opis |
|----------|----------|------|
| `city` | ✅ | Nazwa miasta |
| `stop_id` | ✅ | Identyfikator przystanku |

**Przykładowe żądanie**

```
GET /api/getScheduleForStop.json?city=Gdańsk&stop_id=123
```

**Odpowiedź**

```json
{
  "city": "Gdańsk",
  "stop_id": "123",
  "schedule": [
    {
      "feed": "ZTM_GDA",
      "trip_id": "trip_001",
      "route_id": "1",
      "route_short_name": "1",
      "headsign": "Oliwa",
      "arrival_time": "08:15:00",
      "departure_time": "08:15:30",
      "block_id": "B01",
      "date": "20250601",
      "status": "upcoming",
      "real_arrival": "08:17:00",
      "real_departure": "08:17:30",
      "delay_seconds": 90,
      "rt_source": "trip_update"
    }
  ]
}
```

**Pola realtime w każdym odjeździe**

| Pole | Typ | Opis |
|------|-----|------|
| `status` | `string` | `passed` / `current` / `upcoming` |
| `real_arrival` | `string \| null` | Przewidywany czas przyjazdu (`HH:MM:SS`) |
| `real_departure` | `string \| null` | Przewidywany czas odjazdu (`HH:MM:SS`) |
| `delay_seconds` | `int` | Opóźnienie w sekundach; wartość ujemna oznacza jazdę *przed* rozkładem |
| `rt_source` | `string` | `trip_update` / `estimated` / `static` |

> Dane realtime są pobierane **raz na feed**, a nie na każdy kurs, co minimalizuje liczbę żądań do zewnętrznych API.

---

### getTripDetails.json

Zwraca szczegóły kursu (lub wszystkich kursów dla danej daty) wraz z przystankami, kształtem trasy i danymi realtime.

**Parametry zapytania**

| Parametr | Wymagany | Opis |
|----------|----------|------|
| `city` | ✅ | Nazwa miasta |
| `feed_name` | ✅ | Nazwa feedu GTFS (np. `ZTM_GDA`) |
| `date` | ✅ | Data w formacie `YYYYMMDD` |
| `trip_id` | ❌ | Identyfikator kursu — jeśli pominięty, zwracane są wszystkie kursy dla daty |

**Tryby działania**

- *Tryb pojedynczy* (`trip_id` podany) — zwraca jeden obiekt kursu.
- *Tryb zbiorczy* (`trip_id` pominięty) — zwraca listę wszystkich kursów aktywnych w podanej dacie.

**Przykładowe żądania**

```
GET /api/getTripDetails.json?city=Gdańsk&feed_name=ZTM_GDA&date=20250601&trip_id=trip_001
GET /api/getTripDetails.json?city=Gdańsk&feed_name=ZTM_GDA&date=20250601
```

**Odpowiedź — tryb pojedynczy**

```json
{
  "trip_id": "trip_001",
  "date": "20250601",
  "route_id": "1",
  "block_id": "B01",
  "headsign": "Oliwa",
  "stops": [ /* lista stop_times ze statiku GTFS */ ],
  "shape": [ /* punkty kształtu trasy */ ],
  "realtime": {
    "vehicle_number": "V42",
    "lat": 54.356,
    "lon": 18.643,
    "stop_times": [
      {
        "stop_id": "123",
        "stop_sequence": 1,
        "scheduled_arrival": "08:15:00",
        "scheduled_departure": "08:15:30",
        "real_arrival": "08:17:00",
        "real_departure": "08:17:30",
        "delay_seconds": 90,
        "status": "passed",
        "source": "trip_update"
      }
    ]
  }
}
```

**Odpowiedź — tryb zbiorczy**

```json
{
  "city": "Gdańsk",
  "feed": "ZTM_GDA",
  "date": "20250601",
  "count": 45,
  "trips": [ /* lista obiektów jak w trybie pojedynczym */ ]
}
```

**Priorytety danych realtime na przystankach**

1. **TripUpdates** — jeśli feed RT zawiera delay dla danego przystanku.
2. **Estymacja z pozycji pojazdu** — algorytm haversine + propagacja opóźnienia.
3. **Dane statyczne** — ostateczny fallback; `delay_seconds = 0`, `source = "static"`.

---

### getLineBrigadeDelayForTrip.json

Uproszczona wersja [`getTripDetails.json`](#gettripdetailsjson) — zwraca wyłącznie bieżące opóźnienie kursu bez pełnych danych przystankowych.

**Parametry zapytania**

| Parametr | Wymagany | Opis |
|----------|----------|------|
| `city` | ✅ | Nazwa miasta |
| `feed_name` | ✅ | Nazwa feedu GTFS |
| `date` | ✅ | Data w formacie `YYYYMMDD` |
| `trip_id` | ❌ | Identyfikator kursu; pominięcie zwraca dane dla wszystkich kursów |

**Przykładowe żądanie**

```
GET /api/getLineBrigadeDelayForTrip.json?city=Gdańsk&feed_name=ZTM_GDA&date=20250601&trip_id=trip_001
```

**Odpowiedź — tryb pojedynczy**

```json
{
  "trip_id": "trip_001",
  "route_id": "1",
  "block_id": "B01",
  "delay_seconds": 90,
  "rt_source": "trip_update"
}
```

**Odpowiedź — tryb zbiorczy**

```json
{
  "city": "Gdańsk",
  "feed": "ZTM_GDA",
  "date": "20250601",
  "count": 45,
  "trips": [
    {
      "trip_id": "trip_001",
      "route_id": "1",
      "block_id": "B01",
      "delay_seconds": 90,
      "rt_source": "trip_update"
    }
  ]
}
```

> `delay_seconds` wynosi `null` gdy brak jakichkolwiek danych RT. `rt_source` wynosi `null` w tym samym przypadku.

**Hierarchia wyznaczania opóźnienia**

1. Ostatni dostępny delay z **TripUpdates** (`rt_source = "trip_update"`).
2. Estymacja z **pozycji pojazdu** via haversine (`rt_source = "estimated"`).
3. Brak danych → `delay_seconds = null`, `rt_source = null`.

---

### getTripByVehicle.json

Zwraca szczegóły kursu aktualnie wykonywanego przez podany pojazd. Wyszukiwanie odbywa się przez feed **VehiclePositions** — endpoint odnajduje encję, w której `vehicle.vehicle.id` zgadza się z podanym `vehicle_id`, a następnie pobiera `trip_id` z pola `vehicle.trip.trip_id` i zwraca pełne dane kursu w formacie identycznym jak [`getTripDetails.json`](#gettripdetailsjson) w trybie pojedynczym.

**Parametry zapytania**

| Parametr | Wymagany | Opis |
|----------|----------|------|
| `feed_name` | ✅ | Nazwa feedu GTFS (np. `ZTM_GDA`) |
| `vehicle_id` | ✅ | Identyfikator pojazdu (`vehicle.vehicle.id`, **nie** `label`) |

> ⚠️ Wyszukiwanie odbywa się po polu `vehicle.vehicle.id`, a nie po `vehicle.vehicle.label`. W razie wątpliwości sprawdź surowe dane RT przez [`getParsedRealtimeForFeed.json`](#getparsedrealtimeforfeedjson).

**Przykładowe żądanie**

```
GET /api/getTripByVehicle.json?feed_name=ZTM_GDA&vehicle_id=1234
```

**Odpowiedź — pojazd znaleziony**

```json
{
  "trip_id": "trip_001",
  "route_id": "1",
  "block_id": "B01",
  "headsign": "Oliwa",
  "stops": [ /* lista stop_times ze statiku GTFS */ ],
  "shape": [ /* punkty kształtu trasy */ ],
  "realtime": {
    "vehicle_number": "1234",
    "lat": 54.356,
    "lon": 18.643,
    "stop_times": [
      {
        "stop_id": "123",
        "stop_sequence": 1,
        "scheduled_arrival": "08:15:00",
        "scheduled_departure": "08:15:30",
        "real_arrival": "08:17:00",
        "real_departure": "08:17:30",
        "delay_seconds": 90,
        "status": "passed",
        "source": "trip_update"
      }
    ]
  }
}
```

**Odpowiedź — pojazd nieaktywny lub brak danych** *(HTTP 404)*

```json
{
  "feed_name": "ZTM_GDA",
  "vehicle_id": "1234",
  "found": false,
  "message": "Pojazd nie jest aktualnie aktywny lub nie ma danych w VehiclePositions."
}
```

> Struktura obiektu `realtime.stop_times` jest identyczna z odpowiedzią [`getTripDetails.json`](#gettripdetailsjson). Dane RT (TripUpdates, pozycja pojazdu) są przetwarzane przez tę samą funkcję `build_stop_times_with_realtime`, więc obowiązuje ta sama hierarchia źródeł i obsługa ujemnego `delay_seconds`.

---

### getRouteDetails.json

Zwraca szczegóły linii komunikacyjnej. Bez podania `route_id` zwraca listę wszystkich linii w feedzie.

**Parametry zapytania**

| Parametr | Wymagany | Opis |
|----------|----------|------|
| `city` | ✅ | Nazwa miasta |
| `feed_name` | ✅ | Nazwa feedu GTFS |
| `route_id` | ❌ | Identyfikator linii; pominięcie zwraca listę wszystkich linii |

**Odpowiedź — tryb zbiorczy**

```json
{
  "city": "Gdańsk",
  "feed": "ZTM_GDA",
  "count": 80,
  "routes": [
    {
      "city": "Gdańsk",
      "feed": "ZTM_GDA",
      "route_id": "1",
      "route_short_name": "1",
      "route_type": "3",
      "color": "FF0000",
      "text_color": "FFFFFF"
    }
  ]
}
```

**Odpowiedź — tryb pojedynczy**

```json
{
  "city": "Gdańsk",
  "feed": "ZTM_GDA",
  "route_id": "1",
  "route_short_name": "1",
  "route_type": "3",
  "color": "FF0000",
  "text_color": "FFFFFF",
  "directions": {
    "0": {
      "headsign": "Oliwa",
      "stops": [ /* stop_times pierwszego kursu kierunku 0 */ ],
      "shape": [ /* punkty kształtu */ ]
    },
    "1": {
      "headsign": "Chełm",
      "stops": [],
      "shape": []
    }
  }
}
```

---

### getBlockScheduleForRoute.json

Zwraca mapę brygad (bloków) dla danej linii z przypisanymi kursami pogrupowanymi wg dat.

**Parametry zapytania**

| Parametr | Wymagany | Opis |
|----------|----------|------|
| `city` | ✅ | Nazwa miasta |
| `feed_name` | ✅ | Nazwa feedu GTFS |
| `route_id` | ✅ | Identyfikator linii |

**Przykładowe żądanie**

```
GET /api/getBlockScheduleForRoute.json?city=Gdańsk&feed_name=ZTM_GDA&route_id=1
```

**Odpowiedź**

```json
{
  "city": "Gdańsk",
  "feed": "ZTM_GDA",
  "route_id": "1",
  "blocks": {
    "B01": {
      "20250601": ["trip_001", "trip_005"],
      "20250602": ["trip_002"]
    }
  }
}
```

---

### getTheoriticalBlockDetails.json

Zwraca szczegółowy plan dnia dla danej brygady (bloku), opcjonalnie filtrowany do konkretnej daty.

**Parametry zapytania**

| Parametr | Wymagany | Opis |
|----------|----------|------|
| `city` | ✅ | Nazwa miasta |
| `feed_name` | ✅ | Nazwa feedu GTFS |
| `block_id` | ✅ | Identyfikator brygady / bloku |
| `date` | ❌ | Data w formacie `YYYYMMDD`; brak = wszystkie daty kursowania |

**Odpowiedź**

```json
{
  "city": "Gdańsk",
  "feed": "ZTM_GDA",
  "block_id": "B01",
  "dates": [
    {
      "date": "20250601",
      "start_time": "05:10:00",
      "end_time": "22:45:00",
      "courses": [
        {
          "trip_id": "trip_001",
          "route_id": "1",
          "route_short_name": "1",
          "headsign": "Oliwa",
          "start_time": "05:10:00",
          "end_time": "05:58:00",
          "stops": [ /* lista stop_times */ ]
        }
      ]
    }
  ]
}
```

> Kursy w każdym dniu są posortowane rosnąco wg `start_time`. Pola `start_time` i `end_time` na poziomie dnia odpowiadają pierwszemu i ostatniemu kursowi brygady.

---

### getBlocksForFeedAndDate.json

Zwraca wszystkie brygady aktywne w danym feedzie w podanej dacie wraz z przypisanymi kursami posortowanymi chronologicznie.

**Parametry zapytania**

| Parametr | Wymagany | Opis |
|----------|----------|------|
| `city` | ✅ | Nazwa miasta |
| `feed_name` | ✅ | Nazwa feedu GTFS |
| `date` | ✅ | Data w formacie `YYYYMMDD` |

**Odpowiedź**

```json
{
  "city": "Gdańsk",
  "feed": "ZTM_GDA",
  "date": "20250601",
  "blocks": {
    "B01": [
      {
        "trip_id": "trip_001",
        "route_id": "1",
        "route_short_name": "1",
        "headsign": "Oliwa",
        "order": 0
      },
      {
        "trip_id": "trip_005",
        "route_id": "1",
        "route_short_name": "1",
        "headsign": "Chełm",
        "order": 1
      }
    ]
  }
}
```

> Pole `order` to indeks kursu (od `0`) w ramach danej brygady, wynikający z sortowania wg czasu odjazdu z pierwszego przystanku.

---

### getParsedRealtimeForFeed.json

Pobiera i parsuje wszystkie feedy realtime skonfigurowane dla danego `GTFSFeed`, a następnie łączy je w jeden ujednolicony JSON kompatybilny z **GTFS-RT**.

**Parametry zapytania**

| Parametr | Wymagany | Opis |
|----------|----------|------|
| `feed_name` | ✅ | Nazwa feedu (np. `ZTM_GDA`) |

**Obsługiwane formaty źródłowe**

- Pliki `.pb` (protobuf) — vehicle positions, trip updates, alerts jako osobne URL-e lub jeden wspólny plik.
- Feedy w formacie **JSON** (camelCase i snake_case).

**Zachowanie przy braku VehiclePositions**

- Jeżeli feed RT zawiera **TripUpdates**, ale nie dostarcza `VehiclePositions`, serwer:
  - korzysta z danych statycznych GTFS (`trips.txt`, `stop_times.txt`, `stops.txt`, `shapes.txt`) oraz z `TripUpdates`,
  - dla aktywnych kursów wyznacza przybliżoną aktualną pozycję pojazdu na trasie,
  - generuje **syntetyczne encje `vehicle`** w unified feedzie (`entity[].vehicle`), tak aby struktura była kompatybilna z GTFS-RT.
- Syntetyczne pojazdy:
  - posiadają identyfikator `vehicle.vehicle.id` w postaci `EST-<trip_id>`,
  - mają wypełnione pole `position.latitude` / `position.longitude` na podstawie interpolacji po `shape_dist_traveled` (z fallbackiem do interpolacji między przystankami),
  - opcjonalnie mają uzupełnione `currentStopSequence` na podstawie ostatniego minionego / najbliższego przystanku.

**Odpowiedź gdy brak skonfigurowanych URL-i RT**

```json
{
  "feed_name": "ZTM_GDA",
  "realtime_available": false,
  "message": "Ten feed nie ma skonfigurowanych żadnych linków realtime."
}
```

**Odpowiedź gdy dane RT są dostępne**

```json
{
  "feed_name": "ZTM_GDA",
  "realtime_available": true,
  "header": {
    "gtfsRealtimeVersion": "2.0",
    "incrementality": "FULL_DATASET",
    "timestamp": 1748760000
  },
  "entity": [
    {
      "id": "V42",
      "vehicle": {
        "trip": { "tripId": "trip_001", "routeId": "1" },
        "vehicle": { "id": "V42", "label": "Autobus 42" },
        "position": {
          "latitude": 54.356,
          "longitude": 18.643,
          "bearing": 90.0,
          "calculatedBearing": 92.3
        },
        "currentStopSequence": 5,
        "timestamp": 1748760000
      }
    }
  ],
  "fetch_errors": []
}
```

**Pole `calculatedBearing`**

Każdy pojazd posiada pole `position.calculatedBearing` obliczane po stronie serwera na podstawie różnicy między bieżącą a poprzednią pozycją (buforowaną w pamięci procesu). Wartość `null` oznacza pierwszą obserwację lub brak ruchu pojazdu.

> ⚠️ Cache pozycji pojazdów (`_vehicle_position_cache`) jest resetowany przy każdym restarcie serwera.

---

## Typy danych i słownik pól

| Pole | Format | Opis |
|------|--------|------|
| `date` | `YYYYMMDD` (string) | Data kursowania |
| `arrival_time` / `departure_time` | `HH:MM:SS` | Czas rozkładowy; może przekroczyć `23:59:59` (np. `25:10:00` dla kursów nocnych) |
| `real_arrival` / `real_departure` | `HH:MM:SS \| null` | Przewidywany czas rzeczywisty |
| `delay_seconds` | `int` | Opóźnienie w sekundach; **ujemna wartość** = jazda przed rozkładem |
| `status` | `string` | `passed` — przystanek miniony, `current` — najbliższy pojazdowi, `upcoming` — jeszcze przed pojazdem |
| `source` / `rt_source` | `string` | `trip_update` — dane z RT feed, `estimated` — estymacja haversine, `static` — dane rozkładowe |
| `block_id` | `string` | Identyfikator brygady/bloku; `"N/A"` gdy brak danych |
| `calculatedBearing` | `float \| null` | Kierunek jazdy w stopniach (0° = północ, 90° = wschód) |

---

## Kody błędów

Wszystkie błędy zwracają JSON w postaci `{"status": "error", "message": "..."}`.

| Kod HTTP | Opis |
|----------|------|
| `400` | Brakujące lub nieprawidłowe parametry zapytania |
| `404` | Nie znaleziono zasobu (miasto, feed, kurs, linia) |

---

## Funkcje wewnętrzne

Poniższe funkcje są używane wewnętrznie przez endpointy i nie są bezpośrednio dostępne przez HTTP.

### Pomocniki czasu i geometrii

- **`parse_date_from_time(date, time_str)`** — tworzy `datetime` z uwzględnieniem godzin powyżej `24:00` (kursy nocne).
- **`time_to_seconds(time_str)`** — konwertuje `HH:MM:SS` (w tym `>24h`) na liczbę sekund od początku doby; zwraca `None` przy błędzie.
- **`seconds_to_time(secs)`** — odwrotna konwersja sekund na `HH:MM:SS`; obsługuje wartości powyżej `86400`.
- **`haversine(lat1, lon1, lat2, lon2)`** — odległość w kilometrach między dwoma punktami GPS (wzór haversine).
- **`_compute_bearing(lat1, lon1, lat2, lon2)`** — kierunek jazdy (0–360°) obliczony metodą *forward azimuth*.

### Realtime

- **`load_realtime(feed)`** — pobiera feedy RT dla danego `GTFSFeed`; zwraca dict z kluczami `vehicle_positions`, `trip_updates`, `alerts`.
- **`extract_vehicle_for_trip(realtime, trip_id)`** — wyciąga pozycję pojazdu (`vehicle_number`, `lat`, `lon`) dla podanego kursu.
- **`extract_trip_updates_for_trip(realtime, trip_id)`** — wyciąga listę `stop_time_update` z TripUpdates.

### Estymacja opóźnienia

- **`build_stop_times_with_realtime(...)`** — buduje pełną listę przystanków kursu wzbogaconą o dane RT; kluczowa funkcja łącząca wszystkie źródła danych.
- **`get_single_stop_realtime(...)`** — oblicza `status`, `delay_seconds`, `real_arrival`, `real_departure` dla jednego przystanku jednego kursu.
- **`_find_nearest_stop(...)`** — zwraca indeks przystanku najbliższego aktualnej pozycji pojazdu.
- **`_resolve_delay_for_stop(...)`** — wyznacza `(delay, source)` dla przystanku wg hierarchii: TripUpdates → fallback z pozycji → dane statyczne.

### Kalendarze

- **`_build_service_dates_map(services_by_id, calendar_dates, start_from)`** — buduje mapę `service_id → [daty]` z uwzględnieniem wyjątków z `calendar_dates.txt`.