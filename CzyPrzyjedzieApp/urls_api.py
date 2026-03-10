# CzyPrzyjedzieApp/urls_api.py
from django.urls import path
from . import api

urlpatterns = [
    # Przystanki
    path("getStopsForCity.json", api.get_stops_for_city),

    # Linie / trasy
    path("getRoutesForCity.json", api.get_routes_for_city),
    path("getRouteDetails.json", api.get_route_details),

    # Rozkłady i kursy
    path("getScheduleForStop.json", api.get_schedule_for_stop),
    path("getTripDetails.json", api.get_trip_details),
    path("getTripByVehicle.json", api.get_trip_by_vehicle),

    # Brygady
    path("getBlockScheduleForRoute.json", api.get_block_schedule_for_route),
    path("getTheoriticalBlockDetails.json", api.get_theoritical_block_details),
    path("getBlocksForFeedAndDate.json", api.get_blocks_for_feed_and_date),

    # Opóźnienia
    path("getLineBrigadeDelayForTrip.json", api.get_line_brigade_delay_for_trip),

    # Realtime (debug / podgląd)
    path("getParsedRealtimeForFeed.json", api.get_parsed_realtime_for_feed),
]