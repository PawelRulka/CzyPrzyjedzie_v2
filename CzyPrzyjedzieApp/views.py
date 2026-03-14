from django.shortcuts import render, get_object_or_404
from django.http import JsonResponse
from .models import City, GTFSFeed, Stop


def city_selection(request):
    cities = City.objects.all()
    context = {
        'cities': cities,
        'map_center': [19.1451, 52.2297],
        'map_zoom': 5.5
    }
    return render(request, 'index.html', context)


def city_detail(request, city_slug, vehicle_id=None, feed_name=None, trip_id=None, date=None):
    city = get_object_or_404(City, name=city_slug)
    context = {
        'city': city,
        'selected_vehicle_id': vehicle_id,
        'has_vehicle_view': bool(vehicle_id),
        'selected_feed_name': feed_name,
        'selected_trip_id': trip_id,
        'selected_date': date,
        'has_trip_view': bool(trip_id),
    }
    return render(request, 'city_detail.html', context)


def get_stops_api(request, city_slug):
    city = get_object_or_404(City, name=city_slug)
    feeds = GTFSFeed.objects.filter(city=city, is_active=True)

    stops_list = []
    for feed in feeds:
        stops = Stop.objects.filter(feed=feed)
        for stop in stops:
            stops_list.append({
                'id': stop.stop_id,
                'name': stop.stop_name,
                'lat': float(stop.stop_lat),
                'lon': float(stop.stop_lon),
                'route_types': stop.route_types
            })

    return JsonResponse({'stops': stops_list})