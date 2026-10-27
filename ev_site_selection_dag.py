"""
ev_site_selection_dag.py — Airflow DAG for the NY EV site-selection ETL.

Wraps the shared logic in ``ev_pipeline.py`` (which must be importable — put it on
the PYTHONPATH or in a package Airflow can see) as scheduled tasks. Each task is
idempotent and file-cache-backed: extracts write to EV_WORKDIR, transform/score/export
read from it, so tasks hand off via the filesystem rather than XCom payloads.

Matches the Q1 ETL sprint design:
    extract (DAC / EV regs / stations / grid / charge-ready)  --> in parallel
        --> transform_integrate --> score --> export

Deploy:
    1. Copy this file into your Airflow ``dags_folder``.
    2. Put ``ev_pipeline.py`` on the PYTHONPATH (e.g. same folder + a .pth, or pip-install
       it as a small package).
    3. Set Airflow Variables / env for the pipeline:
         NREL_API_KEY                 (your key; falls back to DEMO_KEY)
         CONED_HOSTING_CAPACITY_URL   (ArcGIS layer url; blank => documented proxy)
         EV_WORKDIR                   (shared working dir the workers can all reach)
    4. Unpause the DAG.
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

import ev_pipeline  # shared ingest/clean/score logic

default_args = {
    "owner": "cjf-ny",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    dag_id="ev_site_selection_ny",
    description="Ingest DAC + EV + stations + Con Ed grid + Charge Ready NY, score, export ranked sites",
    schedule="@weekly",                      # data refreshes slowly; weekly is plenty
    start_date=pendulum.datetime(2026, 1, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ev", "site-selection", "dac", "coned", "cjf-ny"],
) as dag:

    # ---- extract (parallel; each caches to EV_WORKDIR) --------------------
    extract_dac = PythonOperator(
        task_id="extract_dac", python_callable=ev_pipeline.extract_dac,
    )
    extract_evreg = PythonOperator(
        task_id="extract_ev_registrations", python_callable=ev_pipeline.extract_evreg,
    )
    extract_stations = PythonOperator(
        task_id="extract_stations", python_callable=ev_pipeline.extract_stations,
    )
    extract_grid = PythonOperator(
        task_id="extract_coned_grid", python_callable=ev_pipeline.extract_grid,
    )
    extract_charge_ready = PythonOperator(
        task_id="extract_charge_ready_ny", python_callable=ev_pipeline.extract_charge_ready,
    )

    # ---- transform / score / export (sequential) -------------------------
    transform = PythonOperator(
        task_id="transform_integrate", python_callable=ev_pipeline.transform_integrate,
    )
    score = PythonOperator(
        task_id="score_sites", python_callable=ev_pipeline.score,
    )
    export = PythonOperator(
        task_id="export_ranked_recommendations", python_callable=ev_pipeline.export,
    )

    # ---- dependencies ----------------------------------------------------
    extracts = [extract_dac, extract_evreg, extract_stations,
                extract_grid, extract_charge_ready]
    extracts >> transform >> score >> export
