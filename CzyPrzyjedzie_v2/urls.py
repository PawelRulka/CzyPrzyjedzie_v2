from django.contrib import admin
from django.urls import path, include
from CzyPrzyjedzieApp import views

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', views.city_selection, name='city_selection'),
    path('api/<slug:city_slug>/stops/', views.get_stops_api, name='get_stops_api'),
    path('<slug:city_slug>/routes/', views.routes_list, name='city_routes_list'),
    path('<slug:city_slug>/routes/<str:feed_name>/<path:route_id>/', views.route_detail, name='city_route_detail'),
    path('<slug:city_slug>/routes/<str:feed_name>/<path:route_id>/brigades/', views.route_brigades, name='city_route_brigades'),
    path('<slug:city_slug>/routes/<str:feed_name>/<path:route_id>/brigades/<str:block_id>/', views.brigade_detail, name='city_brigade_detail'),
    # Widok brygady per pojazd / kurs (block_id wykrywany automatycznie w JS)
    # UWAGA: te route'y muszą być PRZED route'm dla pojazdu, bo <path:vehicle_id>
    # mogłoby "pożreć" segment '/brigade'.
    path('<slug:city_slug>/vehicle/<path:vehicle_id>/brigade/', views.city_detail, name='city_vehicle_brigade_detail'),
    path('<slug:city_slug>/trip/<str:feed_name>/<str:trip_id>/<str:date>/brigade/', views.city_detail, name='city_trip_brigade_detail'),
    path('<slug:city_slug>/vehicle/<path:vehicle_id>/', views.city_detail, name='city_vehicle_detail'),
    path('<slug:city_slug>/trip/<str:feed_name>/<str:trip_id>/<str:date>/', views.city_detail, name='city_trip_detail'),
    path('<slug:city_slug>/', views.city_detail, name='city_detail'),
    path("api/", include("CzyPrzyjedzieApp.urls_api")),
]