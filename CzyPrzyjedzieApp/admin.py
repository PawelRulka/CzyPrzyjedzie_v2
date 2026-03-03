from django.contrib import admin
from .models import City, GTFSFeed, GTFSSettings, Stop


class GTFSFeedInline(admin.TabularInline):
    model = GTFSFeed
    extra = 1
    show_change_link = True


@admin.register(City)
class CityAdmin(admin.ModelAdmin):
    list_display = (
        "display_name",
        "name",
        "country_icon",
        "latitude",
        "longitude",
        "start_zoom",
    )
    search_fields = ("display_name", "name")
    prepopulated_fields = {"name": ("display_name",)}
    inlines = [GTFSFeedInline]


@admin.register(GTFSFeed)
class GTFSFeedAdmin(admin.ModelAdmin):
    list_display = (
        "name",
        "city",
        "is_active",
        "last_updated",
    )
    list_filter = (
        "city",
        "is_active",
    )
    search_fields = (
        "name",
        "city__display_name",
    )

    actions = ["parse_gtfs_static"]

    def parse_gtfs_static(self, request, queryset):
        from django.core.management import call_command

        for feed in queryset:
            call_command("parse_gtfs", feed.id)

        self.message_user(
            request,
            f"Parsowanie GTFS Static uruchomione dla {queryset.count()} feedów"
        )

    parse_gtfs_static.short_description = "Parsuj GTFS Static"


@admin.register(GTFSSettings)
class GTFSSettingsAdmin(admin.ModelAdmin):
    list_display = (
        "feed",
        "apply_to_all_cities",
    )
    filter_horizontal = (
        "additional_cities",
    )


@admin.register(Stop)
class StopAdmin(admin.ModelAdmin):
    list_display = (
        "stop_name",
        "stop_id",
        "feed",
        "stop_lat",
        "stop_lon",
        "route_types",
    )
    list_filter = (
        "feed",
    )
    search_fields = (
        "stop_name",
        "stop_id",
    )