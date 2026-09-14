"""
CityPulse PySpark transformation job
-------------------------------------
Reads staging.weather_raw via JDBC, cleans and aggregates it, and writes
two warehouse fact tables back to Postgres:
  - fact_weather_hourly   one cleaned row per city per hourly reading
  - fact_weather_daily    one row per city per day (min/max/avg temp, etc.)

Write strategy: OVERWRITE (with truncate=true, so the table and its
constraints are kept instead of Spark dropping/recreating the table).
We reprocess the full staging history every run rather than incrementally
upserting. For an hourly feed from 5 cities the data volume stays tiny
(a few MB even after months), a full recompute is trivial for Spark, and
it avoids an entire class of bugs that come from partial/duplicate
upserts. staging.weather_raw is the immutable source of truth and cheap
to scan in full, so a full-refresh batch pattern is justified here.
"""
import logging

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

JDBC_URL = "jdbc:postgresql://postgres:5432/weatherdb"
JDBC_PROPS = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver",
}

# (weather_code list, category label) - from Open-Meteo's WMO weather code table
WEATHER_CODE_MAP = [
    ([0], "Clear"),
    ([1, 2, 3], "Partly cloudy / Overcast"),
    ([45, 48], "Fog"),
    (list(range(51, 68)), "Drizzle / Rain"),
    (list(range(71, 78)), "Snow"),
    ([80, 81, 82], "Rain showers"),
    (list(range(95, 100)), "Thunderstorm"),
]


def weather_code_to_category(code_col):
    expr = F.lit("Unknown")
    for codes, category in WEATHER_CODE_MAP:
        expr = F.when(code_col.isin(codes), F.lit(category)).otherwise(expr)
    return expr


def main():
    spark = (
        SparkSession.builder.appName("CityPulseTransform")
        .master("local[*]")
        .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.3.jar")
        .getOrCreate()
    )

    logger.info("Reading staging.weather_raw via JDBC")
    raw = spark.read.jdbc(url=JDBC_URL, table="staging.weather_raw", properties=JDBC_PROPS)

    # raw_payload is stored as jsonb; Spark's JDBC reader brings it back as a
    # string, so parse it into real typed columns.
    payload_schema = (
        "city_name STRING, time STRING, temperature_2m DOUBLE, "
        "relative_humidity_2m DOUBLE, precipitation DOUBLE, weather_code INT, "
        "wind_speed_10m DOUBLE"
    )
    parsed = raw.withColumn(
        "payload", F.from_json(F.col("raw_payload").cast("string"), payload_schema)
    )

    cleaned = (
        parsed.select(
            F.col("payload.city_name").alias("city_name"),
            F.to_timestamp("payload.time").alias("reading_time"),
            F.col("payload.temperature_2m").alias("temperature_c"),
            F.col("payload.relative_humidity_2m").alias("humidity_pct"),
            F.col("payload.precipitation").alias("precipitation_mm"),
            F.col("payload.wind_speed_10m").alias("wind_speed_kmh"),
            F.col("payload.weather_code").alias("weather_code"),
        )
        # Can't analyse a reading with no timestamp or temperature - drop it.
        .filter(
            F.col("city_name").isNotNull()
            & F.col("reading_time").isNotNull()
            & F.col("temperature_c").isNotNull()
        )
    )

    # De-duplicate: same city + reading_time can appear more than once if the
    # ingest DAG or the backfill DAG is ever re-run over an overlapping window.
    window = Window.partitionBy("city_name", "reading_time").orderBy(F.lit(1))
    deduped = (
        cleaned.withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # A missing precipitation/wind value means "no rain recorded", not
    # "invalid reading" - fill rather than drop the whole row.
    deduped = deduped.fillna({"precipitation_mm": 0.0, "wind_speed_kmh": 0.0})
    deduped = deduped.withColumn("weather_category", weather_code_to_category(F.col("weather_code")))

    # --- join to dim_city (already seeded by init SQL) ---------------------
    dim_city = spark.read.jdbc(url=JDBC_URL, table="warehouse.dim_city", properties=JDBC_PROPS)
    dim_city = dim_city.select("city_id", "city_name")

    hourly = deduped.join(dim_city, on="city_name", how="inner").select(
        "city_id", "reading_time", "temperature_c", "humidity_pct",
        "precipitation_mm", "wind_speed_kmh", "weather_category",
    )
    hourly.cache()

    logger.info("Writing warehouse.fact_weather_hourly (%s rows)", hourly.count())
    (
        hourly.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "warehouse.fact_weather_hourly")
        .option("user", JDBC_PROPS["user"])
        .option("password", JDBC_PROPS["password"])
        .option("driver", JDBC_PROPS["driver"])
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )

    # --- fact_weather_daily -------------------------------------------------
    daily_agg = (
        hourly.withColumn("reading_date", F.to_date("reading_time"))
        .groupBy("city_id", "reading_date")
        .agg(
            F.min("temperature_c").alias("min_temperature_c"),
            F.max("temperature_c").alias("max_temperature_c"),
            F.avg("temperature_c").alias("avg_temperature_c"),
            F.sum("precipitation_mm").alias("total_precipitation_mm"),
            F.avg("wind_speed_kmh").alias("avg_wind_speed_kmh"),
        )
    )

    # Dominant weather category per city per day = category with the most
    # hourly readings that day.
    category_counts = (
        hourly.withColumn("reading_date", F.to_date("reading_time"))
        .groupBy("city_id", "reading_date", "weather_category")
        .count()
    )
    cat_window = Window.partitionBy("city_id", "reading_date").orderBy(F.desc("count"))
    dominant_category = (
        category_counts.withColumn("rn", F.row_number().over(cat_window))
        .filter(F.col("rn") == 1)
        .select("city_id", "reading_date", F.col("weather_category").alias("dominant_weather_category"))
    )

    daily = daily_agg.join(dominant_category, on=["city_id", "reading_date"], how="left")

    logger.info("Writing warehouse.fact_weather_daily (%s rows)", daily.count())
    (
        daily.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "warehouse.fact_weather_daily")
        .option("user", JDBC_PROPS["user"])
        .option("password", JDBC_PROPS["password"])
        .option("driver", JDBC_PROPS["driver"])
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )

    spark.stop()
    logger.info("Transform job complete.")


if __name__ == "__main__":
    main()
