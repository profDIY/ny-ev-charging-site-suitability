from rest_framework import serializers

from .models import SiteRecommendation, SyncRun


class SiteRecommendationSerializer(serializers.ModelSerializer):
    class Meta:
        model = SiteRecommendation
        fields = [
            "id", "rank", "geoid", "county", "city_town", "is_dac",
            "suitability_score", "population_count", "pop_density", "ev_per_1k",
            "stations_within_r", "grid_headroom_mw", "lat", "lon",
            "equity_dac_n", "ev_demand_n", "traffic_n",
            "grid_capacity_n", "accessibility_n", "infra_gap_n",
        ]


class SyncRunSerializer(serializers.ModelSerializer):
    class Meta:
        model = SyncRun
        fields = ["id", "started_at", "finished_at", "status", "source", "n_sites", "message", "manifest"]
