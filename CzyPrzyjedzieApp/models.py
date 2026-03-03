from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator


class City(models.Model):
    name = models.SlugField(
        max_length=100,
        unique=True,
        help_text="Używane w URL, np. 'bialystok'"
    )
    display_name = models.CharField(
        max_length=150,
        verbose_name="Nazwa wyświetlana"
    )
    description = models.TextField(
        blank=True,
        null=True
    )
    country_icon = models.CharField(
        max_length=10,
        default="🇵🇱"
    )
    country_name = models.CharField(
        max_length=100,
        default="Polska"
    )

    # Pozycja startowa mapy
    latitude = models.DecimalField(
        max_digits=9,
        decimal_places=6
    )
    longitude = models.DecimalField(
        max_digits=9,
        decimal_places=6
    )
    start_zoom = models.IntegerField(
        default=12,
        validators=[MinValueValidator(0), MaxValueValidator(22)]
    )

    def __str__(self):
        return self.display_name

    class Meta:
        verbose_name = "Miasto"
        verbose_name_plural = "Miasta"


class GTFSFeed(models.Model):
    city = models.ForeignKey(
        City,
        on_delete=models.CASCADE,
        related_name="feeds"
    )
    name = models.CharField(
        max_length=100,
        help_text="np. ZTM Białystok"
    )

    # GTFS Static
    static_url = models.URLField(
        verbose_name="GTFS Static URL"
    )

    # GTFS Realtime (osobne feedy zgodnie ze specyfikacją)
    vehicle_positions_url = models.URLField(
        verbose_name="GTFS Realtime VehiclePositions URL",
        blank=True,
        null=True
    )
    trip_updates_url = models.URLField(
        verbose_name="GTFS Realtime TripUpdates URL",
        blank=True,
        null=True
    )
    service_alerts_url = models.URLField(
        verbose_name="GTFS Realtime ServiceAlerts URL",
        blank=True,
        null=True
    )

    is_active = models.BooleanField(
        default=True
    )
    last_updated = models.DateTimeField(
        blank=True,
        null=True
    )

    def __str__(self):
        return f"{self.name} ({self.city.display_name})"

    class Meta:
        verbose_name = "Feed GTFS"
        verbose_name_plural = "Feedy GTFS"


class GTFSSettings(models.Model):
    feed = models.OneToOneField(
        GTFSFeed,
        on_delete=models.CASCADE,
        related_name="settings"
    )
    apply_to_all_cities = models.BooleanField(
        default=False,
        verbose_name="Działa w każdym mieście"
    )
    additional_cities = models.ManyToManyField(
        City,
        blank=True,
        related_name="additional_feeds",
        verbose_name="Dodatkowe miasta"
    )

    def __str__(self):
        return f"Ustawienia dla {self.feed.name}"

    class Meta:
        verbose_name = "Ustawienia GTFS"
        verbose_name_plural = "Ustawienia GTFS"


class Stop(models.Model):
    feed = models.ForeignKey(
        GTFSFeed,
        on_delete=models.CASCADE,
        related_name="stops"
    )
    stop_id = models.CharField(
        max_length=255
    )
    stop_name = models.CharField(
        max_length=255
    )
    stop_lat = models.DecimalField(
        max_digits=9,
        decimal_places=6
    )
    stop_lon = models.DecimalField(
        max_digits=9,
        decimal_places=6
    )

    # np. [3] = autobus, [0] = tramwaj, [0, 3] = oba
    route_types = models.JSONField(
        default=list
    )

    class Meta:
        verbose_name = "Przystanek"
        verbose_name_plural = "Przystanki"
        unique_together = ("feed", "stop_id")

    def __str__(self):
        return f"{self.stop_name} ({self.stop_id})"