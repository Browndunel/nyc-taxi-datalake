"""
Ingestion Bronze de la météo historique horaire de New York, via l'API
Open-Meteo Historical Weather (gratuite, sans clé, couvre 1940 à
aujourd'hui — largement suffisant pour nos données taxi qui démarrent en
2009).

Choix de persistance : la source est une API qui répond en JSON. On ne
persiste pas le JSON brut tel quel (ce serait fidèle à la source mais
inefficace à relire ensuite avec Spark) : on le convertit en Parquet
colonnaire immédiatement après réception, mois par mois — c'est la seule
transformation qu'on s'autorise ici (un simple changement de format, pas
une règle métier), et elle est cohérente avec le fait que Silver/Gold
liront cette météo par de très nombreux jobs Spark. Idempotence assurée
de la même façon que pour taxi_ingest.py : marker _SUCCESS par partition
year/month.
"""
import argparse
import calendar
import logging
import os
import sys
from datetime import date

import pandas as pd
import requests
from hdfs_paths import get_client, weather_partition_dir, is_ingested, write_success_marker, ensure_dir

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("weather_ingest")

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"

# Coordonnées NYC (Manhattan, Central Park — station de référence usuelle
# pour la météo "ville de New York").
LATITUDE = 40.7128
LONGITUDE = -74.0060

HOURLY_VARS = [
    "temperature_2m",
    "precipitation",
    "rain",
    "snowfall",
    "weathercode",
    "windspeed_10m",
    "relativehumidity_2m",
]

CACHE_DIR = os.environ.get("CACHE_DIR", "/tmp/nyc_taxi_cache")
os.makedirs(CACHE_DIR, exist_ok=True)
DATA_START_YEAR = 2009  # aligné sur le début des données Yellow taxi


def month_range(start_year, start_month, end_year, end_month):
    y, m = start_year, start_month
    while (y, m) <= (end_year, end_month):
        yield y, m
        m += 1
        if m > 12:
            m = 1
            y += 1


def fetch_month(year, month):
    last_day = calendar.monthrange(year, month)[1]
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "start_date": f"{year:04d}-{month:02d}-01",
        "end_date": f"{year:04d}-{month:02d}-{last_day:02d}",
        "hourly": ",".join(HOURLY_VARS),
        "timezone": "America/New_York",
    }
    resp = requests.get(OPEN_METEO_URL, params=params, timeout=60)
    resp.raise_for_status()
    payload = resp.json()

    hourly = payload["hourly"]
    df = pd.DataFrame(hourly)
    df.rename(columns={"time": "datetime_local"}, inplace=True)
    df["datetime_local"] = pd.to_datetime(df["datetime_local"])
    return df, resp.url


def ingest_month(client, year, month, force=False):
    partition_dir = weather_partition_dir(year, month)

    if not force and is_ingested(client, partition_dir):
        log.info("SKIP météo %04d-%02d (déjà ingéré)", year, month)
        return "skipped"

    log.info("Récupération météo %04d-%02d...", year, month)
    df, url = fetch_month(year, month)

    local_path = f"{CACHE_DIR}/weather_{year:04d}-{month:02d}.parquet"
    df.to_parquet(local_path, index=False)

    ensure_dir(client, partition_dir)
    hdfs_path = f"{partition_dir}/weather_{year:04d}-{month:02d}.parquet"
    client.upload(hdfs_path, local_path, overwrite=True)

    write_success_marker(client, partition_dir, {
        "year": year,
        "month": month,
        "source_url": url,
        "row_count": len(df),
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
    })
    log.info("OK météo %04d-%02d : %d lignes", year, month, len(df))

    import os
    os.remove(local_path)
    return "ingested"


# Par défaut, on aligne la météo ingérée sur le même périmètre que le taxi
# (mêmes 4 années x mois 01-06) : c'est ce qui sera effectivement joint en
# Silver, pas la peine d'aller chercher plus large par défaut. --full
# bascule sur tout l'historique 2009-aujourd'hui si besoin.
DEFAULT_YEARS = [2009, 2016, 2019, 2025]
DEFAULT_START_MONTH = 1
DEFAULT_END_MONTH = 6


def main():
    today = date.today()
    parser = argparse.ArgumentParser(description="Ingestion Bronze météo (Open-Meteo).")
    parser.add_argument("--years", type=str, default=",".join(str(y) for y in DEFAULT_YEARS))
    parser.add_argument("--start-month", type=int, default=DEFAULT_START_MONTH)
    parser.add_argument("--end-month", type=int, default=DEFAULT_END_MONTH)
    parser.add_argument("--full", action="store_true",
                         help="ingère tout l'historique %d-aujourd'hui au lieu du périmètre par défaut" % DATA_START_YEAR)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--max-months", type=int, default=None)
    args = parser.parse_args()

    client = get_client()
    stats = {"ingested": 0, "skipped": 0}
    processed = 0

    if args.full:
        months_to_process = list(month_range(DATA_START_YEAR, 1, today.year, today.month))
    else:
        years = [int(y.strip()) for y in args.years.split(",") if y.strip()]
        months_to_process = [(y, m) for y in years for m in range(args.start_month, args.end_month + 1)]

    for year, month in months_to_process:
        result = ingest_month(client, year, month, force=args.force)
        stats[result] = stats.get(result, 0) + 1
        processed += 1
        if args.max_months and processed >= args.max_months:
            log.info("Limite --max-months=%d atteinte, arrêt.", args.max_months)
            break

    log.info("Résumé : %s", stats)


if __name__ == "__main__":
    sys.exit(main())
