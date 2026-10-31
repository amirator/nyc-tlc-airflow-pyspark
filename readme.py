import json, pathlib

def render(k):
    pct = lambda x: f"{x*100:.1f}%"
    b = k["borough_share"]; dq = k["dq_by_reason"]; mon = k["monthly"]
    hi = max(mon, key=lambda r: r[1]); lo = min(mon, key=lambda r: r[1])
    pay = {p: (n, r) for p, n, r in k["payment"]}
    z = k["top_zones"]
    rules = "\n".join(f"| `{r}` | {n:,} |" for r, n in sorted(dq.items(), key=lambda x: -x[1]) if r != "valid")
    return f"""# NYC TLC batch pipeline: Airflow 3 + PySpark

A monthly batch pipeline over **{k['raw_rows']:,} real NYC green-taxi trip records** ({k['months'][0]} to {k['months'][1]}). It runs resumable ingest, then a PySpark transform with rule-based data-quality flags, then a reconciliation gate that fails the run if the numbers don't add up. It is orchestrated by an Airflow 3 TaskFlow DAG.

## Business problem
An operations or data team receives the TLC's monthly files and needs a repeatable job. The job should pick up the new month, absorb schema drift between files, quarantine bad records with a reason, and only publish curated data and aggregates once the row counts reconcile.

## Data
- **NYC Taxi & Limousine Commission, Trip Record Data (green taxi)**: monthly parquet files from the [TLC Trip Record Data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (`d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_YYYY-MM.parquet`).
- **TLC Taxi Zone Lookup** (`taxi_zone_lookup.csv`): 265 zones mapped to boroughs.
- Raw files are not committed. `tlc/ingest.py` downloads them (resuming dropped connections with HTTP Range requests) and writes a SHA-256 manifest.

## Pipeline
```
target_month ──> download ──> spark_transform ──> quality_gate
 (logical-2mo)   (resumable,    (normalise schema,    (raw = valid + rejected,
                  sha256)        DQ flags, curated     curated = valid, aggs reconcile,
                                 parquet + 4 aggs)     reject rate <= 10%, else fail)
```
- **Airflow 3.1** (`dags/tlc_green_monthly.py`): TaskFlow API, `@monthly`, retries with delay, `max_active_runs=1`, and XCom hand-offs. Because the TLC publishes with a lag of about two months, a run for month M processes month M-2. **`airflow dags test` was run locally and all 4 tasks succeeded** (see `reports/airflow_dags_test.log`).
- **PySpark 3.5** (`tlc/spark_job.py`): casts each month to a single column contract, since the files drift in types and columns (for example `cbd_congestion_fee` is new in 2025). It applies 7 data-quality rules where the first match wins, uses broadcast joins to the zone and payment lookups, writes curated parquet partitioned by month, and builds aggregates by month×borough, zone, hour×weekend and payment type.
- **Quality gate** (`tlc/quality.py`): reads the Spark outputs back with DuckDB and raises an error, so Airflow marks the task failed, if any reconciliation check fails.
- `run.py` backfills Jan-Sep 2026 using the same functions, then generates the KPIs, chart and this README.

![dashboard](reports/dashboard.png)

## Key findings (green taxi, {k['months'][0]} to {k['months'][1]})
- **{k['valid_trips']:,} valid trips** out of {k['raw_rows']:,} raw rows. **{k['rejected']:,} ({pct(k['reject_rate'])}) were quarantined**, mostly for zero or negative distance ({dq.get('zero_or_negative_distance',0):,}) and trips longer than 6 hours ({dq.get('duration_over_6h',0):,}).
- **${k['revenue']:,.0f}** in total charges, averaging {k['avg_miles']} mi and {k['avg_minutes']} min per trip.
- Green cabs serve the outer boroughs and upper Manhattan: Manhattan accounts for {pct(b.get('Manhattan',0))} of pickups (led by **{z[0][0]}** with {z[0][2]:,} and {z[1][0]} with {z[1][2]:,}), followed by Queens at {pct(b.get('Queens',0))} and Brooklyn at {pct(b.get('Brooklyn',0))}.
- The weekday peak pickup hour is **{k['weekday_peak_hour'][0]}:00**. The busiest month was {hi[0]} ({hi[1]:,}) and the quietest was {lo[0]} ({lo[1]:,}).
- The tip rate on credit cards is **{pct(pay['Credit card'][1])}**. Cash tips are not recorded, so the blended rate of {pct(k['tip_rate'])} understates real tipping.

| DQ rule | rows rejected |
|---|---|
{rules}

## Skills shown
Apache Airflow 3 (TaskFlow, scheduling, retries, XCom, `dags test`) · PySpark (schema contracts, DataFrame API, broadcast joins, partitioned parquet, caching) · data-quality engineering and reconciliation · resumable HTTP ingest · DuckDB · pytest (Spark tests on synthetic fixtures, DAG integrity test) · GitHub Actions (separate Spark and Airflow jobs).

## How to run
```bash
pip install -r requirements.txt          # needs Java 17 for Spark
python run.py                            # download + Spark backfill + gate + KPIs + README
python -m pytest -q
# Airflow (separate env, official constraints):
pip install "apache-airflow==3.1.0" --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.1.0/constraints-3.12.txt
AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags airflow dags test tlc_green_monthly 2026-11-01
```

## Honest caveats
- Spark runs in `local[*]` mode on a single machine, not on a cluster (no EMR/Databricks/Dataproc). The code is cluster-agnostic, but it has not been run at cluster scale.
- Airflow was run with `airflow dags test` (in-process), not as a deployed scheduler or worker.
- Green-taxi volume is modest (about 0.4M rows); yellow-taxi files were too slow to download on the build machine.
- This is an independent portfolio project using public data, not work for the TLC.

MIT licence · Amir Khan
"""

def write():
    pathlib.Path("README.md").write_text(render(json.loads(pathlib.Path("reports/kpis.json").read_text())))
