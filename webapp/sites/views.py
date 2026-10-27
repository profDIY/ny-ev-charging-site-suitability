import json

import requests
from django.conf import settings
from django.core.management import call_command
from django.db.models import Avg, Count, Q
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET, require_POST
from rest_framework import viewsets
from rest_framework.exceptions import ParseError
from rest_framework.pagination import LimitOffsetPagination

from .folium_utils import build_suitability_map
from .geo import haversine_km
from .models import SiteRecommendation, SyncRun, TractScore
from .serializers import SiteRecommendationSerializer, SyncRunSerializer

PHOTON_URL = "https://photon.komoot.io/api/"
# Bias suggestions toward the project's Con Edison / downstate-NY service territory.
NY_BIAS = {"lat": 40.75, "lon": -73.95}


def latest_successful_run():
    return SyncRun.objects.filter(status="success").order_by("-started_at").first()


# --------------------------------------------------------------------- pages
def dashboard(request):
    run = latest_successful_run()
    return render(request, "sites/dashboard.html", {"latest_run": run})


def map_view(request):
    run = latest_successful_run()
    if run is None:
        return HttpResponse(
            "<h3 style='font-family:sans-serif;padding:2rem'>No data synced yet. "
            "Use the \"Sync now\" button on the dashboard, or run "
            "<code>python manage.py sync_sites</code>.</h3>"
        )

    top_sites = list(
        SiteRecommendation.objects.filter(sync_run=run).order_by("rank").values(
            "rank", "geoid", "county", "is_dac", "suitability_score",
            "ev_per_1k", "stations_within_r", "lat", "lon",
        )
    )
    tract_scores = dict(
        TractScore.objects.filter(sync_run=run).values_list("geoid", "suitability_score")
    )

    geojson_path = settings.REPO_ROOT / "data" / "cache" / "dac_2023_geojson.json"
    dac_geojson = json.loads(geojson_path.read_text()) if geojson_path.exists() else {"type": "FeatureCollection", "features": []}

    coverage_km = (run.manifest or {}).get("coverage_radius_km", 2.0)
    fmap = build_suitability_map(top_sites, tract_scores, dac_geojson, coverage_radius_km=coverage_km)
    return HttpResponse(fmap.get_root().render())


# --------------------------------------------------------------------- REST API
class SiteRecommendationViewSet(viewsets.ReadOnlyModelViewSet):
    serializer_class = SiteRecommendationSerializer
    pagination_class = LimitOffsetPagination

    def get_queryset(self):
        run = latest_successful_run()
        qs = SiteRecommendation.objects.filter(sync_run=run) if run else SiteRecommendation.objects.none()

        county = self.request.query_params.get("county")
        if county:
            qs = qs.filter(county__iexact=county)

        is_dac = self.request.query_params.get("is_dac")
        if is_dac is not None:
            qs = qs.filter(is_dac=is_dac.lower() in ("1", "true", "yes"))

        min_score = self.request.query_params.get("min_score")
        if min_score:
            try:
                qs = qs.filter(suitability_score__gte=float(min_score))
            except ValueError:
                raise ParseError("min_score must be numeric")

        ordering = self.request.query_params.get("ordering", "rank")
        allowed = {"rank", "-rank", "suitability_score", "-suitability_score", "county", "-county"}
        if ordering in allowed:
            qs = qs.order_by(ordering)
        return qs


class SyncRunViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = SyncRun.objects.all()[:20]
    serializer_class = SyncRunSerializer


@require_GET
def stats_view(request):
    run = latest_successful_run()
    if run is None:
        return JsonResponse({"has_data": False})

    sites = SiteRecommendation.objects.filter(sync_run=run)
    agg = sites.aggregate(
        avg_score=Avg("suitability_score"),
        n=Count("id"),
        n_dac=Count("id", filter=Q(is_dac=True)),
    )
    county_breakdown = list(
        sites.values("county")
        .annotate(n=Count("id"), avg_score=Avg("suitability_score"))
        .order_by("-n")
    )

    return JsonResponse({
        "has_data": True,
        "n_sites": agg["n"],
        "n_dac": agg["n_dac"],
        "dac_pct": round(100 * agg["n_dac"] / agg["n"], 1) if agg["n"] else 0,
        "avg_score": round(agg["avg_score"], 1) if agg["avg_score"] is not None else None,
        "n_tracts_scored": TractScore.objects.filter(sync_run=run).count(),
        "county_breakdown": county_breakdown,
        "last_sync": {
            "started_at": run.started_at.isoformat(),
            "source": run.source,
            "sources": (run.manifest or {}).get("sources"),
            "weights": (run.manifest or {}).get("weights"),
        },
    })


@require_POST
def sync_view(request):
    full = request.POST.get("full") == "1" or request.GET.get("full") == "1"
    try:
        call_command("sync_sites", full=full)
    except Exception as exc:
        run = SyncRun.objects.order_by("-started_at").first()
        return JsonResponse({
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "run": SyncRunSerializer(run).data if run else None,
        }, status=500)

    run = SyncRun.objects.order_by("-started_at").first()
    return JsonResponse({"ok": True, "run": SyncRunSerializer(run).data})


@require_GET
def dac_geojson_view(request):
    """Serves the cached DAC tract boundaries the dashboard's map fetches directly
    (REPO_ROOT/data/cache, not under webapp/, so it needs its own route)."""
    geojson_path = settings.REPO_ROOT / "data" / "cache" / "dac_2023_geojson.json"
    if not geojson_path.exists():
        return JsonResponse({"type": "FeatureCollection", "features": []})
    return HttpResponse(geojson_path.read_text(encoding="utf-8"), content_type="application/json")


@require_GET
def geocode_view(request):
    """Autocomplete proxy: our JS never talks to Photon directly (keeps one
    place to swap providers / add caching, and avoids client-side CORS)."""
    q = request.GET.get("q", "").strip()
    if len(q) < 3:
        return JsonResponse({"results": []})
    try:
        resp = requests.get(PHOTON_URL, params={
            "q": q, "limit": 6, "lang": "en",
            "lat": NY_BIAS["lat"], "lon": NY_BIAS["lon"],
        }, timeout=6, headers={"User-Agent": "ev-site-selection-webapp/1.0"})
        resp.raise_for_status()
        feats = resp.json().get("features", [])
    except Exception:
        return JsonResponse({"results": [], "error": "geocoder unavailable"})

    results = []
    for f in feats:
        props = f.get("properties", {})
        coords = f.get("geometry", {}).get("coordinates")
        if not coords:
            continue
        label = ", ".join(str(props[k]) for k in ("name", "street", "city", "state") if props.get(k))
        label = label or props.get("name") or q
        results.append({"label": label, "lat": coords[1], "lon": coords[0]})
    return JsonResponse({"results": results})


@require_GET
def nearest_view(request):
    try:
        lat = float(request.GET["lat"])
        lon = float(request.GET["lon"])
        radius_km = float(request.GET.get("radius_km", 2.0))
    except (KeyError, ValueError):
        return JsonResponse({"error": "lat and lon are required and must be numeric"}, status=400)

    run = latest_successful_run()
    if run is None:
        return JsonResponse({})

    best, best_d = None, None
    for s in SiteRecommendation.objects.filter(sync_run=run).values(
        "rank", "geoid", "county", "is_dac", "suitability_score", "lat", "lon"
    ):
        d = haversine_km(lat, lon, s["lat"], s["lon"])
        if best_d is None or d < best_d:
            best, best_d = s, d
    if best is None:
        return JsonResponse({})

    return JsonResponse({
        **best,
        "distance_km": round(best_d, 3),
        "within_coverage": best_d <= radius_km,
    })
