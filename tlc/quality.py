"""Data-quality gate run after the Spark job; raises so Airflow marks the task failed."""
import duckdb

class QualityError(Exception):
    pass

def gate(out_dir: str, counts: dict, max_reject_rate: float = 0.10) -> dict:
    con = duckdb.connect()
    dq = dict(con.execute(f"select coalesce(dq_reason,'valid'), sum(count) from '{out_dir}/dq_summary/*.parquet' group by 1").fetchall())
    curated = con.execute(f"select count(*) from read_parquet('{out_dir}/curated_trips/*/*.parquet')").fetchone()[0]
    agg = con.execute(f"select sum(trips) from '{out_dir}/agg_month_borough/*.parquet'").fetchone()[0]
    raw = sum(dq.values()); rejected = raw - dq.get("valid", 0)
    checks = {"raw_equals_valid_plus_rejected": raw == counts["raw"] and dq.get("valid", 0) == counts["valid"],
              "curated_equals_valid": curated == counts["valid"],
              "aggregates_reconcile": agg == counts["valid"],
              "reject_rate_ok": rejected / raw <= max_reject_rate}
    failed = [k for k, ok in checks.items() if not ok]
    if failed:
        raise QualityError(f"failed checks: {failed}")
    return {"raw": raw, "rejected": rejected, "reject_rate": round(rejected / raw, 4), "by_reason": {k: int(v) for k, v in sorted(dq.items())}}
