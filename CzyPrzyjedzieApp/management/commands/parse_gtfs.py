import csv
import io
import zipfile
import requests
from django.core.management.base import BaseCommand
from django.utils import timezone
from CzyPrzyjedzieApp.models import GTFSFeed, Stop


class Command(BaseCommand):
    help = 'Parsuje GTFS Static feed i zapisuje przystanki do bazy danych'

    def add_arguments(self, parser):
        parser.add_argument('feed_id', type=int, help='ID feeda GTFS do sparsowania')

    def handle(self, *args, **options):
        feed_id = options['feed_id']

        try:
            feed = GTFSFeed.objects.get(id=feed_id)
        except GTFSFeed.DoesNotExist:
            self.stdout.write(self.style.ERROR(f'Feed o ID {feed_id} nie istnieje'))
            return

        self.stdout.write(f'Rozpoczynam parsowanie GTFS dla: {feed.name}')

        try:
            # Pobierz GTFS ZIP
            self.stdout.write('Pobieranie pliku GTFS...')
            response = requests.get(feed.static_url, timeout=60)
            response.raise_for_status()

            # Rozpakuj ZIP
            zip_file = zipfile.ZipFile(io.BytesIO(response.content))

            # Wczytaj stops.txt
            self.stdout.write('Parsowanie stops.txt...')
            stops_data = {}
            with zip_file.open('stops.txt') as f:
                reader = csv.DictReader(io.TextIOWrapper(f, 'utf-8'))
                for row in reader:
                    stops_data[row['stop_id']] = {
                        'name': row['stop_name'],
                        'lat': float(row['stop_lat']),
                        'lon': float(row['stop_lon']),
                        'route_types': set()
                    }

            # Wczytaj routes.txt
            self.stdout.write('Parsowanie routes.txt...')
            routes_data = {}
            with zip_file.open('routes.txt') as f:
                reader = csv.DictReader(io.TextIOWrapper(f, 'utf-8'))
                for row in reader:
                    routes_data[row['route_id']] = int(row['route_type'])

            # Wczytaj trips.txt (połączenie route -> trip)
            self.stdout.write('Parsowanie trips.txt...')
            trips_data = {}
            with zip_file.open('trips.txt') as f:
                reader = csv.DictReader(io.TextIOWrapper(f, 'utf-8'))
                for row in reader:
                    trips_data[row['trip_id']] = row['route_id']

            # Wczytaj stop_times.txt i połącz dane
            self.stdout.write('Parsowanie stop_times.txt...')
            with zip_file.open('stop_times.txt') as f:
                reader = csv.DictReader(io.TextIOWrapper(f, 'utf-8'))
                for row in reader:
                    stop_id = row['stop_id']
                    trip_id = row['trip_id']

                    if stop_id in stops_data and trip_id in trips_data:
                        route_id = trips_data[trip_id]
                        if route_id in routes_data:
                            route_type = routes_data[route_id]
                            stops_data[stop_id]['route_types'].add(route_type)

            # Usuń stare przystanki dla tego feeda
            self.stdout.write('Usuwanie starych danych...')
            Stop.objects.filter(feed=feed).delete()

            # Zapisz nowe przystanki
            self.stdout.write('Zapisywanie przystanków do bazy...')
            stops_to_create = []
            for stop_id, data in stops_data.items():
                if data['route_types']:  # Tylko przystanki z przypisanymi trasami
                    stops_to_create.append(Stop(
                        feed=feed,
                        stop_id=stop_id,
                        stop_name=data['name'],
                        stop_lat=data['lat'],
                        stop_lon=data['lon'],
                        route_types=sorted(list(data['route_types']))
                    ))

            Stop.objects.bulk_create(stops_to_create, batch_size=1000)

            # Aktualizuj timestamp
            feed.last_updated = timezone.now()
            feed.save()

            self.stdout.write(self.style.SUCCESS(
                f'Sukces! Zapisano {len(stops_to_create)} przystanków dla {feed.name}'
            ))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Błąd podczas parsowania: {str(e)}'))
            raise