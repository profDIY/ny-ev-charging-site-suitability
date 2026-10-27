from django.urls import include, path
from rest_framework.routers import DefaultRouter

from . import views

router = DefaultRouter()
router.register(r"sites", views.SiteRecommendationViewSet, basename="site")
router.register(r"syncs", views.SyncRunViewSet, basename="syncrun")

app_name = "sites"

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("map/", views.map_view, name="map"),
    path("data/cache/dac_2023_geojson.json", views.dac_geojson_view, name="dac-geojson"),
    path("api/", include(router.urls)),
    path("api/stats/", views.stats_view, name="api-stats"),
    path("api/sync/", views.sync_view, name="api-sync"),
    path("api/geocode/", views.geocode_view, name="api-geocode"),
    path("api/nearest/", views.nearest_view, name="api-nearest"),
]
