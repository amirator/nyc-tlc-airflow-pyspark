import datetime as dt, json, os, pathlib, sys
import pytest
ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

pyspark = pytest.importorskip("pyspark")

@pytest.fixture(scope="module")
def spark():
    from tlc.spark_job import spark_session
    s = spark_session("tests"); yield s; s.stop()

def T(s): return dt.datetime.fromisoformat(s)

def make_fixture(spark, path, month_cols_variant=False):
    """Synthetic rows, one per DQ rule plus valid ones (test fixture, not real data)."""
    rows = [  # pickup, dropoff, PU, DO, dist, fare, tip, total, pay
        ("2026-09-02 08:00", "2026-09-02 08:20", 74, 75, 2.0, 12.0, 3.0, 17.0, 1),
        ("2026-09-02 09:00", "2026-09-02 09:10", 75, 74, 1.0, 8.0, 0.0, 9.5, 2),
        ("2026-09-03 18:00", "2026-09-03 18:30", 7, 7, 4.0, 20.0, 4.0, 26.0, 1),
        ("2026-08-31 23:50", "2026-09-01 00:10", 74, 75, 2.0, 12.0, 0.0, 13.0, 1),  # outside month
        ("2026-09-04 10:00", "2026-09-04 09:00", 74, 75, 2.0, 12.0, 0.0, 13.0, 1),  # negative duration
        ("2026-09-04 10:00", "2026-09-04 18:00", 74, 75, 2.0, 12.0, 0.0, 13.0, 1),  # > 6h
        ("2026-09-04 11:00", "2026-09-04 11:05", 74, 75, 0.0, 3.0, 0.0, 4.0, 2),    # zero distance
        ("2026-09-04 12:00", "2026-09-04 12:30", 74, 75, 250.0, 50.0, 0.0, 55.0, 1), # > 100 mi
        ("2026-09-04 13:00", "2026-09-04 13:20", 74, 75, 2.0, -12.0, 0.0, -13.0, 4), # negative fare
        ("2026-09-04 14:00", "2026-09-04 14:20", None, 75, 2.0, 12.0, 0.0, 13.0, 1), # missing location
    ]
    data = [(T(a), T(b), pu, do, d, f, t, tot, p, "2.0" if month_cols_variant else 2) for a, b, pu, do, d, f, t, tot, p, in rows]
    ptype = "string" if month_cols_variant else "long"
    df = spark.createDataFrame(data, f"lpep_pickup_datetime timestamp, lpep_dropoff_datetime timestamp, PULocationID int, DOLocationID int, trip_distance double, fare_amount double, tip_amount double, total_amount double, payment_type long, passenger_count {ptype}")
    df.write.mode("overwrite").parquet(path)

def test_transform_flags_and_reconciles(spark, tmp_path):
    from tlc.spark_job import transform
    from tlc.quality import gate, QualityError
    make_fixture(spark, str(tmp_path / "raw"), month_cols_variant=True)
    zones = tmp_path / "z.csv"
    zones.write_text('"LocationID","Borough","Zone","service_zone"\n7,"Queens","Astoria","Boro Zone"\n74,"Manhattan","East Harlem North","Boro Zone"\n75,"Manhattan","East Harlem South","Boro Zone"\n')
    out = str(tmp_path / "out")
    counts = transform(spark, {"2026-09": str(tmp_path / "raw")}, str(zones), out)
    assert counts == {"raw": 10, "valid": 3}
    with pytest.raises(QualityError):   # 70% rejected > 10% threshold
        gate(out, counts)
    res = gate(out, counts, max_reject_rate=0.9)
    assert res["by_reason"] == {"distance_over_100mi": 1, "duration_over_6h": 1, "missing_location": 1, "negative_fare": 1,
                                "non_positive_duration": 1, "pickup_outside_month": 1, "valid": 3, "zero_or_negative_distance": 1}

def test_resumable_download(tmp_path):
    from tlc import ingest
    blob = b"x" * 1000
    class R:
        def __init__(s, body, headers=None): s.body, s.headers = body, headers or {}
        def raise_for_status(s): pass
        def iter_content(s, n):
            yield s.body[:300]
            if len(s.body) > 300:
                raise ingest.requests.ConnectionError("drop")
        def __enter__(s): return s
        def __exit__(s, *a): pass
    class Sess:
        calls = []
        def head(s, url, **k): return R(b"", {"content-length": str(len(blob))})
        def get(s, url, headers=None, **k):
            start = int(headers["Range"].split("=")[1].rstrip("-")); s.calls.append(start); return R(blob[start:])
    ingest.time.sleep = lambda *_: None
    dest = ingest.download("http://x", tmp_path / "f.bin", session=Sess())
    assert dest.read_bytes() == blob and Sess.calls == [0, 300, 600, 900]

def test_readme_in_sync():
    import readme
    assert (ROOT / "README.md").read_text() == readme.render(json.loads((ROOT / "reports/kpis.json").read_text()))
