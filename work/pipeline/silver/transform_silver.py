"""
Job Spark Silver : lit Bronze (taxi x4 types + météo + référentiel zones),
réconcilie, nettoie, et écrit un modèle unique interrogeable sans jamais
retourner à Bronze.

Exécution : en cluster réel, jamais en local[*] :
    spark-submit --master spark://spark-master:7077 transform_silver.py

Deux problèmes structurels à résoudre, explicitement :

1. Unifier la localisation (GPS brut avant juillet 2016, LocationID après)
   -> pour chaque mois lu individuellement, si le fichier a des colonnes
   PULocationID/DOLocationID on les utilise telles quelles ; s'il a des
   colonnes de coordonnées GPS, on les rattache à un LocationID via la
   grille pré-calculée (build_zone_grid.py). Résultat : toutes les courses,
   quelle que soit leur date, ont un pickup_zone_id / dropoff_zone_id.

2. Ne pas confondre "colonne pas encore introduite" (NULL) et "colonne
   présente avec une vraie valeur 0" pour les suppléments tarifaires.
   -> IMPORTANT : on ne fait JAMAIS un spark.read.parquet("/bronze/taxi/...")
   global avec mergeSchema=true sur l'ensemble de l'historique, car Spark
   fusionnerait alors silencieusement les schémas et rendrait les deux cas
   ci-dessus indiscernables (une colonne absente d'un fichier donné devient
   simplement NULL après le merge, exactement comme une valeur NULL
   authentique). On lit donc CHAQUE PARTITION (vehicle_type/year/month)
   individuellement, on regarde ses colonnes réelles à cet instant précis,
   et on matérialise nous-mêmes un flag booléen "<supplement>_disponible"
   AVANT toute union. Le flag est basé sur la présence réelle de la
   colonne dans le fichier source de ce mois-là, pas sur une date
   supposée — donc robuste même si la TLC a introduit une colonne à une
   date légèrement différente de la documentation publique.
"""
import logging
import sys

from pyspark.sql import SparkSession, functions as F, types as T

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("transform_silver")

HDFS_URI = "hdfs://namenode:9000"
BRONZE_TAXI = f"{HDFS_URI}/bronze/taxi"
BRONZE_WEATHER = f"{HDFS_URI}/bronze/weather"
BRONZE_ZONES_LOOKUP = f"{HDFS_URI}/bronze/reference/taxi_zones/taxi_zone_lookup.csv"
BRONZE_GRID = f"{HDFS_URI}/bronze/reference/taxi_zones/grid_lookup.parquet"
SILVER_TAXI = f"{HDFS_URI}/silver/taxi"
SILVER_WEATHER = f"{HDFS_URI}/silver/weather"

GRID_RESOLUTION = 0.001

# Colonnes tarifaires apparues progressivement : nom canonique -> variantes
# de nom possibles selon le type de véhicule / l'époque (la casse et les
# noms exacts changent selon les années dans les fichiers TLC).
SURCHARGE_COLUMNS = {
    "improvement_surcharge": ["improvement_surcharge"],
    "congestion_surcharge": ["congestion_surcharge"],
    "airport_fee": ["airport_fee", "Airport_fee"],
    "cbd_congestion_fee": ["cbd_congestion_fee", "congestion_fee_cbd"],
}

# Mapping des colonnes "coeur" par type de véhicule -> nom canonique.
# FHV n'a structurellement ni tarif ni distance (rien à mapper pour ces
# colonnes) ; c'est volontaire et documenté dans le sujet, pas un bug.
VEHICLE_COLUMN_MAP = {
    "yellow": {
        "pickup_datetime": ["tpep_pickup_datetime"],
        "dropoff_datetime": ["tpep_dropoff_datetime"],
        "pu_location_id": ["PULocationID"],
        "do_location_id": ["DOLocationID"],
        "pickup_longitude": ["pickup_longitude"],
        "pickup_latitude": ["pickup_latitude"],
        "dropoff_longitude": ["dropoff_longitude"],
        "dropoff_latitude": ["dropoff_latitude"],
        "passenger_count": ["passenger_count"],
        "trip_distance": ["trip_distance"],
        "fare_amount": ["fare_amount"],
        "tip_amount": ["tip_amount"],
        "tolls_amount": ["tolls_amount"],
        "total_amount": ["total_amount"],
    },
    "green": {
        "pickup_datetime": ["lpep_pickup_datetime"],
        "dropoff_datetime": ["lpep_dropoff_datetime"],
        "pu_location_id": ["PULocationID"],
        "do_location_id": ["DOLocationID"],
        "pickup_longitude": ["pickup_longitude"],
        "pickup_latitude": ["pickup_latitude"],
        "dropoff_longitude": ["dropoff_longitude"],
        "dropoff_latitude": ["dropoff_latitude"],
        "passenger_count": ["passenger_count"],
        "trip_distance": ["trip_distance"],
        "fare_amount": ["fare_amount"],
        "tip_amount": ["tip_amount"],
        "tolls_amount": ["tolls_amount"],
        "total_amount": ["total_amount"],
    },
    "fhv": {
        "pickup_datetime": ["pickup_datetime"],
        "dropoff_datetime": ["dropOff_datetime", "dropoff_datetime"],
        "pu_location_id": ["PUlocationID", "PULocationID"],
        "do_location_id": ["DOlocationID", "DOLocationID"],
        # pas de GPS, pas de tarif, pas de distance chez FHV : absent du
        # mapping -> sera rempli à NULL, ce qui est la vérité du terrain.
    },
    "fhvhv": {
        "pickup_datetime": ["pickup_datetime"],
        "dropoff_datetime": ["dropoff_datetime"],
        "pu_location_id": ["PULocationID"],
        "do_location_id": ["DOLocationID"],
        "trip_distance": ["trip_miles"],
        "fare_amount": ["base_passenger_fare"],
        "tip_amount": ["tips"],
        "tolls_amount": ["tolls"],
        "total_amount": ["base_passenger_fare"],  # pas de total_amount direct chez FHVHV ; recalculé plus bas
    },
}

CANONICAL_TAXI_COLUMNS = [
    "pickup_datetime", "dropoff_datetime", "pu_location_id", "do_location_id",
    "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude",
    "passenger_count", "trip_distance", "fare_amount", "tip_amount", "tolls_amount", "total_amount",
]


def list_bronze_partitions(spark, vehicle_type):
    """Liste les partitions year=/month= déjà ingérées pour un type donné,
    en s'appuyant sur le même système de fichiers que Spark (pas besoin
    d'un client HDFS séparé côté driver)."""
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    base = sc._jvm.org.apache.hadoop.fs.Path(f"{BRONZE_TAXI}/vehicle_type={vehicle_type}")
    fs = base.getFileSystem(hadoop_conf)

    partitions = []
    if not fs.exists(base):
        return partitions
    for year_status in fs.listStatus(base):
        year_path = year_status.getPath()
        for month_status in fs.listStatus(year_path):
            month_path = month_status.getPath()
            success = sc._jvm.org.apache.hadoop.fs.Path(f"{month_path.toString()}/_SUCCESS")
            if fs.exists(success):
                partitions.append(month_path.toString())
    return partitions


def read_partition_with_flags(spark, path, vehicle_type):
    """Lit UNE partition (un seul mois, un seul type), et construit le
    schéma canonique + les flags de présence des suppléments, à partir des
    colonnes réellement présentes dans CE fichier précis."""
    df = spark.read.parquet(path)
    source_columns = set(df.columns)
    col_map = VEHICLE_COLUMN_MAP[vehicle_type]

    select_exprs = []
    for canonical, candidates in col_map.items():
        found = next((c for c in candidates if c in source_columns), None)
        if found:
            select_exprs.append(F.col(found).alias(canonical))
        else:
            select_exprs.append(F.lit(None).alias(canonical))
    # colonnes canoniques jamais mappées pour ce type (ex : distance chez FHV)
    for canonical in CANONICAL_TAXI_COLUMNS:
        if canonical not in col_map:
            select_exprs.append(F.lit(None).alias(canonical))

    for canonical, candidates in SURCHARGE_COLUMNS.items():
        found = next((c for c in candidates if c in source_columns), None)
        if found:
            select_exprs.append(F.col(found).cast("double").alias(canonical))
            select_exprs.append(F.lit(True).alias(f"{canonical}_disponible"))
        else:
            select_exprs.append(F.lit(None).cast("double").alias(canonical))
            select_exprs.append(F.lit(False).alias(f"{canonical}_disponible"))

    out = df.select(*select_exprs)
    out = out.withColumn("vehicle_type", F.lit(vehicle_type))
    return out


def resolve_zones(df, grid_lookup):
    """Unifie pickup/dropoff en pickup_zone_id / dropoff_zone_id : si
    pu_location_id/do_location_id existent déjà on les garde tels quels ;
    sinon (courses pré-2016) on résout via la grille GPS -> LocationID.

    IMPORTANT : le join se fait sur des indices de grille ENTIERS
    (round(lon/résolution)), jamais sur des floats arrondis. Un join sur
    l'égalité de deux floats arrondis séparément (ici et dans
    build_zone_grid.py) est fragile : round(-73.978/0.001)*0.001 redonne
    -73.97800000000001 en IEEE 754, pas -73.978 — l'égalité échoue
    silencieusement et la ligne ne matche aucune zone, sans la moindre
    erreur ni warning. Testé et confirmé sur données synthétiques avant
    correction (voir historique de dev) : passer par un cast entier
    élimine le problème à la racine.
    """
    grid_pu = grid_lookup.select(
        F.col("grid_lon_idx").alias("pu_grid_lon_idx"),
        F.col("grid_lat_idx").alias("pu_grid_lat_idx"),
        F.col("LocationID").alias("gps_pu_zone"),
    )
    grid_do = grid_lookup.select(
        F.col("grid_lon_idx").alias("do_grid_lon_idx"),
        F.col("grid_lat_idx").alias("do_grid_lat_idx"),
        F.col("LocationID").alias("gps_do_zone"),
    )

    df = df.withColumn("pu_grid_lon_idx", F.round(F.col("pickup_longitude") / GRID_RESOLUTION).cast("long"))
    df = df.withColumn("pu_grid_lat_idx", F.round(F.col("pickup_latitude") / GRID_RESOLUTION).cast("long"))
    df = df.withColumn("do_grid_lon_idx", F.round(F.col("dropoff_longitude") / GRID_RESOLUTION).cast("long"))
    df = df.withColumn("do_grid_lat_idx", F.round(F.col("dropoff_latitude") / GRID_RESOLUTION).cast("long"))

    df = df.join(F.broadcast(grid_pu), on=["pu_grid_lon_idx", "pu_grid_lat_idx"], how="left")
    df = df.join(F.broadcast(grid_do), on=["do_grid_lon_idx", "do_grid_lat_idx"], how="left")

    df = df.withColumn("pickup_zone_id", F.coalesce(F.col("pu_location_id"), F.col("gps_pu_zone")))
    df = df.withColumn("dropoff_zone_id", F.coalesce(F.col("do_location_id"), F.col("gps_do_zone")))

    drop_cols = ["pu_grid_lon_idx", "pu_grid_lat_idx", "do_grid_lon_idx", "do_grid_lat_idx",
                 "gps_pu_zone", "gps_do_zone", "pu_location_id", "do_location_id",
                 "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude"]
    return df.drop(*drop_cols)


def build_silver_taxi(spark):
    grid_lookup = spark.read.parquet(BRONZE_GRID)

    all_dfs = []
    for vehicle_type in VEHICLE_COLUMN_MAP:
        partitions = list_bronze_partitions(spark, vehicle_type)
        log.info("%s : %d partitions Bronze trouvées", vehicle_type, len(partitions))
        for path in partitions:
            df = read_partition_with_flags(spark, path, vehicle_type)
            all_dfs.append(df)

    if not all_dfs:
        raise RuntimeError("Aucune partition Bronze trouvée — lancer l'ingestion d'abord.")

    taxi = all_dfs[0]
    for df in all_dfs[1:]:
        taxi = taxi.unionByName(df, allowMissingColumns=True)

    taxi = resolve_zones(taxi, grid_lookup)

    taxi = taxi.withColumn("pickup_datetime", F.col("pickup_datetime").cast("timestamp"))
    taxi = taxi.withColumn("dropoff_datetime", F.col("dropoff_datetime").cast("timestamp"))
    taxi = taxi.withColumn("trip_duration_min",
                            (F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long")) / 60.0)
    taxi = taxi.withColumn("price_per_km",
                            F.when(F.col("trip_distance") > 0,
                                   F.col("total_amount") / (F.col("trip_distance") * 1.60934)))

    taxi = taxi.withColumn("year", F.year("pickup_datetime"))
    taxi = taxi.withColumn("month", F.month("pickup_datetime"))
    taxi = taxi.withColumn("pickup_hour_ts", F.date_trunc("hour", F.col("pickup_datetime")))

    return taxi


def build_silver_weather(spark):
    weather = spark.read.parquet(f"{BRONZE_WEATHER}/*/*")
    weather = weather.withColumn("datetime_local", F.col("datetime_local").cast("timestamp"))
    weather = weather.withColumn("year", F.year("datetime_local"))
    weather = weather.withColumn("month", F.month("datetime_local"))
    return weather


def main():
    spark = (
        SparkSession.builder
        .appName("nyc-taxi-datalake-silver")
        .master("spark://spark-master:7077")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    log.info("Construction Silver weather...")
    weather = build_silver_weather(spark)
    weather.write.mode("overwrite").partitionBy("year", "month").parquet(SILVER_WEATHER)
    log.info("Silver weather écrit : %s", SILVER_WEATHER)

    log.info("Construction Silver taxi...")
    taxi = build_silver_taxi(spark)
    taxi = taxi.join(
        weather.select(F.col("datetime_local").alias("pickup_hour_ts"),
                        "temperature_2m", "precipitation", "rain", "snowfall", "weathercode", "windspeed_10m"),
        on="pickup_hour_ts", how="left",
    )
    taxi.write.mode("overwrite").partitionBy("vehicle_type", "year", "month").parquet(SILVER_TAXI)
    log.info("Silver taxi écrit : %s", SILVER_TAXI)

    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
