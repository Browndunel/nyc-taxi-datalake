"""
Gold 5 — Écart de prix (par km) selon la météo, comparé au prix moyen
général.

On catégorise la météo en quelques buckets lisibles (à partir du code
météo Open-Meteo / des mm de précipitation / de neige), puis on compare le
prix moyen par km dans chaque bucket au prix moyen général (toutes
conditions confondues) — l'écart en % est ce qui répond directement à la
question, pas besoin de laisser l'utilisateur recalculer la référence.
"""
import sys
from pyspark.sql import SparkSession, functions as F

HDFS_URI = "hdfs://namenode:9000"
SILVER_TAXI = f"{HDFS_URI}/silver/taxi"
GOLD_OUT = f"{HDFS_URI}/gold/price_by_weather"

MAX_PRICE_PER_KM = 50


def weather_bucket_col():
    return (
        F.when(F.col("snowfall") > 0, "neige")
        .when(F.col("precipitation") > 2, "pluie_forte")
        .when(F.col("precipitation") > 0, "pluie_legere")
        .when(F.col("windspeed_10m") > 40, "vent_fort")
        .otherwise("normal")
    )


def main():
    spark = SparkSession.builder.appName("gold-weather-price").master("spark://spark-master:7077").getOrCreate()
    taxi = spark.read.parquet(SILVER_TAXI)

    base = taxi.filter(
        F.col("price_per_km").isNotNull()
        & (F.col("price_per_km") > 0)
        & (F.col("price_per_km") < MAX_PRICE_PER_KM)
        & F.col("temperature_2m").isNotNull()
    ).withColumn("weather_bucket", weather_bucket_col())

    global_avg = base.agg(F.avg("price_per_km").alias("prix_moyen_general")).collect()[0]["prix_moyen_general"]

    by_bucket = (
        base.groupBy("vehicle_type", "weather_bucket")
        .agg(F.avg("price_per_km").alias("prix_moyen_bucket"), F.count("*").alias("nb_courses"))
        .withColumn("prix_moyen_general", F.lit(global_avg))
        .withColumn("ecart_pct", (F.col("prix_moyen_bucket") - F.col("prix_moyen_general")) / F.col("prix_moyen_general") * 100)
    )

    by_bucket.write.mode("overwrite").parquet(GOLD_OUT)
    print(f"Gold weather price écrit : {GOLD_OUT}")
    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
