"""Local backfill of the same tasks the DAG runs, over Jan-Sep 2026, then KPIs/charts/README."""
import json, pathlib, sys
import duckdb
import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
from tlc.ingest import fetch_month, fetch_zones
from tlc.spark_job import spark_session, transform
from tlc.quality import gate

MONTHS = [f"2026-{m:02d}" for m in range(1, 10)]
OUT = "data/processed/backfill"

def kpis(dq):
    con = duckdb.connect(); q = lambda s: con.execute(s).fetchall()
    P = lambda t: f"'{OUT}/{t}/*.parquet'"
    tot = q(f"select sum(trips), sum(revenue), sum(tips), sum(fare) from {P('agg_month_borough')}")[0]
    bor = q(f"select coalesce(pu_borough,'Unknown'), sum(trips) from {P('agg_month_borough')} group by 1 order by 2 desc")
    mon = q(f"select source_month, sum(trips) from {P('agg_month_borough')} group by 1 order by 1")
    zone = q(f"select pu_zone, pu_borough, trips from {P('agg_zone')} order by trips desc limit 5")
    pay = q(f"select coalesce(payment_desc,'Not recorded'), trips, round(tips/nullif(fare,0),4) from {P('agg_payment')} order by trips desc")
    hr = q(f"select pickup_hour, sum(trips) from {P('agg_hour')} where not is_weekend group by 1 order by 1")
    peak = max(hr, key=lambda r: r[1])
    dur = q(f"select round(sum(avg_minutes*trips)/sum(trips),2), round(sum(avg_miles*trips)/sum(trips),3) from {P('agg_month_borough')}")[0]
    return {"months": [MONTHS[0], MONTHS[-1]], "raw_rows": dq["raw"], "valid_trips": int(tot[0]), "rejected": dq["rejected"],
            "reject_rate": dq["reject_rate"], "dq_by_reason": dq["by_reason"], "revenue": round(tot[1], 0),
            "tip_rate": round(tot[2] / tot[3], 4), "avg_minutes": dur[0], "avg_miles": dur[1],
            "borough_share": {b: round(n / tot[0], 4) for b, n in bor},
            "monthly": [[m, int(n)] for m, n in mon], "top_zones": [[z, b, int(n)] for z, b, n in zone],
            "payment": [[p, int(n), r] for p, n, r in pay], "weekday_peak_hour": [int(peak[0]), int(peak[1])],
            "weekday_hourly": [[int(h), int(n)] for h, n in hr]}

def charts(k):
    fig, ax = plt.subplots(1, 3, figsize=(15, 4.6))
    ax[0].bar([m[5:] for m, _ in k["monthly"]], [n / 1e3 for _, n in k["monthly"]], color="#2f855a"); ax[0].set_title("Valid green-taxi trips per month 2026 (k)")
    ax[1].plot([h for h, _ in k["weekday_hourly"]], [n / 1e3 for _, n in k["weekday_hourly"]], marker="o", color="#2b6cb0"); ax[1].set_title("Weekday trips by pickup hour (k)")
    r = sorted(k["dq_by_reason"].items(), key=lambda x: x[1]); r = [x for x in r if x[0] != "valid"]
    ax[2].barh([a for a, _ in r], [b for _, b in r], color="#c53030"); ax[2].set_title("Rows rejected by DQ rule")
    fig.tight_layout(); fig.savefig("reports/dashboard.png", dpi=110)

if __name__ == "__main__":
    offline = "--offline" in sys.argv
    if not offline:
        fetch_zones(); entries = {m: fetch_month(m)["file"] for m in MONTHS}
    else:
        entries = {m: f"data/raw/green_tripdata_{m}.parquet" for m in MONTHS}
    spark = spark_session("tlc_backfill")
    counts = transform(spark, entries, "data/raw/taxi_zone_lookup.csv", OUT); spark.stop()
    dq = gate(OUT, counts)
    k = kpis(dq); pathlib.Path("reports/kpis.json").write_text(json.dumps(k, indent=2))
    charts(k)
    import readme; readme.write()
