"""PySpark transform: schema normalisation, rule-based data-quality flags, curated parquet + aggregates."""
from pyspark.sql import SparkSession, DataFrame, functions as F, types as T

SCHEMA = {"VendorID": "int", "lpep_pickup_datetime": "timestamp", "lpep_dropoff_datetime": "timestamp",
          "PULocationID": "int", "DOLocationID": "int", "passenger_count": "int", "trip_distance": "double",
          "fare_amount": "double", "tip_amount": "double", "total_amount": "double", "payment_type": "int",
          "trip_type": "int", "congestion_surcharge": "double", "cbd_congestion_fee": "double"}
PAYMENT = {1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute", 5: "Unknown", 6: "Voided"}

def spark_session(app="tlc"):
    return (SparkSession.builder.appName(app).master("local[*]")
            .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "8")
            .config("spark.ui.enabled", "false").getOrCreate())

def normalise(df: DataFrame, month: str) -> DataFrame:
    """Months ship with drifting column types/sets: cast to one contract, add missing cols as null."""
    cols = [(F.col(c).cast(t) if c in df.columns else F.lit(None).cast(t)).alias(c) for c, t in SCHEMA.items()]
    return df.select(*cols).withColumn("source_month", F.lit(month))

def flag_quality(df: DataFrame) -> DataFrame:
    dur = (F.col("lpep_dropoff_datetime").cast("long") - F.col("lpep_pickup_datetime").cast("long")) / 60.0
    df = df.withColumn("duration_min", dur)
    rules = [
        ("pickup_outside_month", F.date_format("lpep_pickup_datetime", "yyyy-MM") != F.col("source_month")),
        ("non_positive_duration", F.col("duration_min") <= 0),
        ("duration_over_6h", F.col("duration_min") > 360),
        ("zero_or_negative_distance", F.col("trip_distance") <= 0),
        ("distance_over_100mi", F.col("trip_distance") > 100),
        ("negative_fare", F.col("fare_amount") < 0),
        ("missing_location", F.col("PULocationID").isNull() | F.col("DOLocationID").isNull()),
    ]
    reason = F.lit(None).cast("string")
    for name, cond in reversed(rules):  # first matching rule wins
        reason = F.when(F.coalesce(cond, F.lit(False)), F.lit(name)).otherwise(reason)
    return df.withColumn("dq_reason", reason)

def transform(spark, inputs: dict, zones_csv: str, out_dir: str) -> dict:
    """inputs: {month: parquet_path}. Writes curated + aggregates under out_dir, returns row counts."""
    parts = [normalise(spark.read.parquet(p), m) for m, p in sorted(inputs.items())]
    df = parts[0]
    for p in parts[1:]:
        df = df.unionByName(p)
    df = flag_quality(df).cache()
    zones = spark.read.option("header", True).csv(zones_csv).select(
        F.col("LocationID").cast("int").alias("PULocationID"), F.col("Borough").alias("pu_borough"), F.col("Zone").alias("pu_zone"))
    pay = spark.createDataFrame([(k, v) for k, v in PAYMENT.items()], "payment_type int, payment_desc string")
    good = (df.filter(F.col("dq_reason").isNull()).join(F.broadcast(zones), "PULocationID", "left")
              .join(F.broadcast(pay), "payment_type", "left")
              .withColumn("pickup_hour", F.hour("lpep_pickup_datetime"))
              .withColumn("is_weekend", F.dayofweek("lpep_pickup_datetime").isin(1, 7)))
    good.write.mode("overwrite").partitionBy("source_month").parquet(f"{out_dir}/curated_trips")
    agg = lambda *k: good.groupBy(*k).agg(F.count("*").alias("trips"), F.round(F.sum("total_amount"), 2).alias("revenue"),
                                          F.round(F.sum("tip_amount"), 2).alias("tips"), F.round(F.sum("fare_amount"), 2).alias("fare"),
                                          F.round(F.avg("trip_distance"), 3).alias("avg_miles"), F.round(F.avg("duration_min"), 2).alias("avg_minutes"))
    agg("source_month", "pu_borough").write.mode("overwrite").parquet(f"{out_dir}/agg_month_borough")
    agg("pu_borough", "pu_zone").write.mode("overwrite").parquet(f"{out_dir}/agg_zone")
    agg("pickup_hour", "is_weekend").write.mode("overwrite").parquet(f"{out_dir}/agg_hour")
    agg("payment_desc").write.mode("overwrite").parquet(f"{out_dir}/agg_payment")
    dq = df.groupBy("source_month", "dq_reason").count()
    dq.write.mode("overwrite").parquet(f"{out_dir}/dq_summary")
    counts = {"raw": df.count(), "valid": good.count()}
    df.unpersist()
    return counts
