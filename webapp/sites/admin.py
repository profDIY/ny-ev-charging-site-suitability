from django.contrib import admin

from .models import SiteRecommendation, SyncRun, TractScore


@admin.register(SyncRun)
class SyncRunAdmin(admin.ModelAdmin):
    list_display = ("id", "started_at", "status", "source", "n_sites", "message")
    list_filter = ("status", "source")


@admin.register(SiteRecommendation)
class SiteRecommendationAdmin(admin.ModelAdmin):
    list_display = ("rank", "geoid", "county", "is_dac", "suitability_score", "sync_run")
    list_filter = ("county", "is_dac", "sync_run")
    search_fields = ("geoid", "county", "city_town")
    ordering = ("rank",)


@admin.register(TractScore)
class TractScoreAdmin(admin.ModelAdmin):
    list_display = ("geoid", "county", "is_dac", "suitability_score", "sync_run")
    list_filter = ("county", "is_dac", "sync_run")
    search_fields = ("geoid", "county")
