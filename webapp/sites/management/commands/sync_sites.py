"""
sync_sites — load the ETL pipeline's ranked site recommendations into the DB.

Two modes:
  python manage.py sync_sites              # fast path: re-read outputs/ already on disk
  python manage.py sync_sites --full       # run ev_pipeline.run_all() first, then load

Both modes read from the SAME outputs/ directory the notebook writes to
(REPO_ROOT/outputs), so the notebook and the web app stay in sync off one
ETL implementation (ev_pipeline.py). Each run is recorded as a SyncRun so
the dashboard can show provenance (LIVE vs SYNTHETIC sources, weights used,
when it last ran).
"""
from __future__ import annotations

import json
import math
import shutil

import pandas as pd
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from sites.models import SiteRecommendation, SyncRun, TractScore


class Command(BaseCommand):
    help = "Sync ranked site recommendations from the ETL pipeline outputs into the database."

    def add_arguments(self, parser):
        parser.add_argument(
            "--full",
            action="store_true",
            help="Run the full ETL pipeline (ev_pipeline.run_all) before syncing, "
                 "instead of just re-reading the existing outputs/ CSVs.",
        )

    def handle(self, *args, **options):
        run = SyncRun.objects.create(
            status="running",
            source="full_pipeline" if options["full"] else "csv_reload",
        )
        try:
            if options["full"]:
                self._run_pipeline()
            n = self._load_outputs(run)
            n_tracts = self._load_tract_scores(run)
            manifest_path = settings.OUTPUTS_DIR / "run_manifest.json"
            manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else None

            run.status = "success"
            run.n_sites = n
            run.manifest = manifest
            run.message = (
                f"Loaded {n} ranked site recommendations and {n_tracts} scored tracts "
                f"from {settings.OUTPUTS_DIR}"
            )
            run.finished_at = timezone.now()
            run.save()
            self._prune_old_runs(keep=5)
            self.stdout.write(self.style.SUCCESS(run.message))
        except Exception as exc:  # keep the dashboard's "sync now" button non-fatal
            run.status = "failed"
            run.message = f"{type(exc).__name__}: {exc}"
            run.finished_at = timezone.now()
            run.save()
            self.stdout.write(self.style.ERROR(run.message))
            raise

    def _prune_old_runs(self, keep: int):
        stale_ids = list(SyncRun.objects.order_by("-started_at").values_list("pk", flat=True)[keep:])
        if stale_ids:
            SyncRun.objects.filter(pk__in=stale_ids).delete()  # cascades to its sites/tract_scores

    def _run_pipeline(self):
        """Run the real ETL pipeline and land its outputs in REPO_ROOT/outputs."""
        import os

        pipeline_workdir = settings.REPO_ROOT / "ev_work"
        os.environ.setdefault("EV_WORKDIR", str(pipeline_workdir))
        import ev_pipeline
        import importlib
        importlib.reload(ev_pipeline)  # pick up EV_WORKDIR if it changed since import

        ev_pipeline.run_all()
        pipeline_outputs = pipeline_workdir / "outputs"
        settings.OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
        for f in pipeline_outputs.glob("*"):
            shutil.copy2(f, settings.OUTPUTS_DIR / f.name)

    def _load_outputs(self, run: SyncRun) -> int:
        csv_path = settings.OUTPUTS_DIR / "ranked_site_recommendations.csv"
        if not csv_path.exists():
            raise FileNotFoundError(
                f"{csv_path} not found — run the notebook (Section 12: Export) "
                f"or `python manage.py sync_sites --full` at least once first."
            )
        df = pd.read_csv(csv_path)
        df = df.where(pd.notnull(df), None)

        rows = []
        for _, r in df.iterrows():
            if r.get("lat") is None or r.get("lon") is None:
                continue
            rows.append(SiteRecommendation(
                sync_run=run,
                rank=int(r["rank"]),
                geoid=str(r["geoid"]),
                county=str(r["county"]),
                city_town=str(r.get("city_town") or ""),
                is_dac=bool(r.get("is_dac")),
                suitability_score=float(r["suitability_score"]),
                population_count=_f(r.get("population_count")),
                pop_density=_f(r.get("pop_density")),
                ev_per_1k=_f(r.get("ev_per_1k")),
                stations_within_r=_i(r.get("stations_within_r")),
                grid_headroom_mw=_f(r.get("grid_headroom_mw")),
                lat=float(r["lat"]),
                lon=float(r["lon"]),
                equity_dac_n=_f(r.get("equity_dac_n")),
                ev_demand_n=_f(r.get("ev_demand_n")),
                traffic_n=_f(r.get("traffic_n")),
                grid_capacity_n=_f(r.get("grid_capacity_n")),
                accessibility_n=_f(r.get("accessibility_n")),
                infra_gap_n=_f(r.get("infra_gap_n")),
            ))

        with transaction.atomic():
            SiteRecommendation.objects.bulk_create(rows)
        return len(rows)

    def _load_tract_scores(self, run: SyncRun) -> int:
        csv_path = settings.OUTPUTS_DIR / "master_integrated_dataset.csv"
        if not csv_path.exists():
            return 0
        df = pd.read_csv(csv_path, usecols=["geoid", "county", "is_dac", "suitability_score"])
        df = df.dropna(subset=["geoid", "suitability_score"])
        rows = [
            TractScore(
                sync_run=run,
                geoid=str(r["geoid"]),
                county=str(r.get("county") or ""),
                is_dac=bool(r.get("is_dac")),
                suitability_score=float(r["suitability_score"]),
            )
            for _, r in df.iterrows()
        ]
        with transaction.atomic():
            TractScore.objects.bulk_create(rows)
        return len(rows)


def _f(v):
    if v is None:
        return None
    try:
        f = float(v)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


def _i(v):
    f = _f(v)
    return None if f is None else int(f)
