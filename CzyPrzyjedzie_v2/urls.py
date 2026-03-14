from django.contrib import admin
from django.urls import path, include
from CzyPrzyjedzieApp import views

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', views.city_selection, name='city_selection'),
    path('api/<slug:city_slug>/stops/', views.get_stops_api, name='get_stops_api'),
    path('<slug:city_slug>/vehicle/<path:vehicle_id>/', views.city_detail, name='city_vehicle_detail'),
    path('<slug:city_slug>/trip/<str:feed_name>/<str:trip_id>/<str:date>/', views.city_detail, name='city_trip_detail'),
    path('<slug:city_slug>/', views.city_detail, name='city_detail'),
    path("api/", include("CzyPrzyjedzieApp.urls_api")),
]