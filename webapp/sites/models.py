from django.db import models


class SyncRun(models.Model):
    """One execution of the ETL -> DB sync, so the dashboard can show provenance."""

    STATUS_CHOICES = [
        ("running", "Running"),
        ("success", "Success"),
        ("failed", "Failed"),
    ]
    SOURCE_CHOICES = [
        ("csv_reload", "Reload latest ETL outputs (CSV/manifest)"),
        ("full_pipeline", "Run full ETL pipeline (ev_pipeline.run_all)"),
    ]

    started_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default="running")
    source = models.CharField(max_length=20, choices=SOURCE_CHOICES, default="csv_reload")
    n_sites = models.IntegerField(default=0)
    message = models.TextField(blank=True, default="")
    manifest = models.JSONField(null=True, blank=True)

    class Meta:
        ordering = ["-started_at"]

    def __str__(self):
        return f"SyncRun #{self.pk} ({self.status}, {self.started_at:%Y-%m-%d %H:%M})"


class SiteRecommendation(models.Model):
    """A ranked, scored EV-charging site recommendation, synced from the ETL pipeline."""

    sync_run = models.ForeignKey(SyncRun, on_delete=models.CASCADE, related_name="sites")

    rank = models.IntegerField(db_index=True)
    geoid = models.CharField(max_length=20, db_index=True)
    county = models.CharField(max_length=64, db_index=True)
    city_town = models.CharField(max_length=128, blank=True, default="")
    is_dac = models.BooleanField(default=False, db_index=True)

    suitability_score = models.FloatField(db_index=True)
    population_count = models.FloatField(null=True, blank=True)
    pop_density = models.FloatField(null=True, blank=True)
    ev_per_1k = models.FloatField(null=True, blank=True)
    stations_within_r = models.IntegerField(null=True, blank=True)
    grid_headroom_mw = models.FloatField(null=True, blank=True)

    lat = models.FloatField()
    lon = models.FloatField()

    # normalized 0-1 sub-criteria, for the score-breakdown UI
    equity_dac_n = models.FloatField(null=True, blank=True)
    ev_demand_n = models.FloatField(null=True, blank=True)
    traffic_n = models.FloatField(null=True, blank=True)
    grid_capacity_n = models.FloatField(null=True, blank=True)
    accessibility_n = models.FloatField(null=True, blank=True)
    infra_gap_n = models.FloatField(null=True, blank=True)

    class Meta:
        ordering = ["rank"]
        indexes = [models.Index(fields=["sync_run", "rank"])]

    def __str__(self):
        return f"#{self.rank} {self.geoid} ({self.county}) score={self.suitability_score:.1f}"


class TractScore(models.Model):
    """Suitability score for every scored census tract (not just the top-N),
    used to draw the full choropleth layer on the map — synced from
    outputs/master_integrated_dataset.csv alongside SiteRecommendation."""

    sync_run = models.ForeignKey(SyncRun, on_delete=models.CASCADE, related_name="tract_scores")
    geoid = models.CharField(max_length=20, db_index=True)
    county = models.CharField(max_length=64, blank=True, default="")
    is_dac = models.BooleanField(default=False)
    suitability_score = models.FloatField()

    class Meta:
        indexes = [models.Index(fields=["sync_run", "geoid"])]

    def __str__(self):
        return f"{self.geoid} score={self.suitability_score:.1f}"
