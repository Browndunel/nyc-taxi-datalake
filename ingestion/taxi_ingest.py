"""
Ingestion Bronze des 4 types de véhicules TLC (yellow, green, fhv, fhvhv).

Idempotent et reprenable : pour chaque (type, année, mois), on vérifie
d'abord le marker _SUCCESS dans HDFS (hdfs_paths.is_ingested) — si présent,
le mois est sauté sans le retélécharger, que le script soit relancé après
un crash, une interruption manuelle (Ctrl+C) ou juste une nouvelle
exécution planifiée.

Chaque type de véhicule a sa propre date de début (cf. table TLC) : on ne
tente donc jamais de télécharger un mois antérieur à la création du
service, ce qui évite des centaines de requêtes vouées à échouer.

Deux sources possibles pour chaque fichier, dans cet ordre de priorité :
  1. Un dossier local déjà fourni (--local-source-dir / $LOCAL_SOURCE_DIR),
     organisé en <year>/<month>/<vehicle_type>_tripdata_<year>-<month>.parquet
     — exactement l'arborescence du dossier nyc_taxi_data fourni pour ce
     projet. Si le fichier y est présent, on l'utilise directement (copie
     locale, aucun accès réseau).
  2. À défaut, téléchargement depuis la source officielle TLC (CloudFront),
     qui republie tout son historique au format Parquet depuis 2009 — donc
     pas besoin de gérer le CSV côté ingestion, Bronze n'est qu'une copie
     fidèle du fichier source, sans transformation, quelle que soit la
     provenance.

Périmètre par défaut : les 4 années fournies dans le dossier local
(2009/2016/2019/2025), mois 01 à 06 — exactement le sous-ensemble remis
pour ce projet, chacune choisie par le prof pour tomber sur une des 4
ruptures de schéma. --years/--start-month/--end-month permettent
d'élargir explicitement (ex: ingérer tout l'historique via le réseau).
"""
import argparse
import logging
import os
import shutil
import sys
from datetime import date

import requests
from hdfs_paths import get_client, taxi_partition_dir, is_ingested, write_success_marker, ensure_dir

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("taxi_ingest")

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Date de début officielle de publication de chaque type de véhicule.
# Confirmé a posteriori contre le dossier local fourni : 2009 ne contient
# que yellow, 2016 ne contient ni fhvhv (créé en 2019), 2019/01 ne contient
# pas encore fhvhv (apparaît à partir de 2019/02 pile) — cohérent avec ces
# dates.
VEHICLE_START = {
    "yellow": (2009, 1),
    "green": (2013, 8),
    "fhv": (2015, 1),
    "fhvhv": (2019, 2),
}

# Le sous-ensemble effectivement fourni pour ce projet (dossier
# nyc_taxi_data) : 4 années choisies pour tomber sur chacune des 4
# ruptures de schéma décrites dans le sujet, 6 mois chacune.
DEFAULT_YEARS = [2009, 2016, 2019, 2025]
DEFAULT_START_MONTH = 1
DEFAULT_END_MONTH = 6

CACHE_DIR = os.environ.get("CACHE_DIR", "/tmp/nyc_taxi_cache")  # /cache (racine) refusé par l'utilisateur non-root du conteneur notebook
os.makedirs(CACHE_DIR, exist_ok=True)
LOCAL_SOURCE_DIR = os.environ.get("LOCAL_SOURCE_DIR", "/local_source")


def month_range(start_month, end_month):
    return range(start_month, end_month + 1)


def local_path_for(vehicle_type, year, month):
    """Chemin attendu dans le dossier local fourni, si monté. Retourne
    None si le dossier local n'est pas monté ou si le fichier n'y est
    pas — dans ce cas on retombera sur le téléchargement réseau."""
    if not os.path.isdir(LOCAL_SOURCE_DIR):
        return None
    candidate = os.path.join(LOCAL_SOURCE_DIR, f"{year:04d}", f"{month:02d}",
                              f"{vehicle_type}_tripdata_{year:04d}-{month:02d}.parquet")
    return candidate if os.path.isfile(candidate) else None


def get_month_file(vehicle_type, year, month):
    """Retourne (local_path, source) où source est 'local' ou 'network',
    ou (None, url) si le fichier n'existe nulle part (mois pas encore
    publié côté TLC)."""
    local_src = local_path_for(vehicle_type, year, month)
    if local_src:
        fname = os.path.basename(local_src)
        cached = f"{CACHE_DIR}/{fname}"
        shutil.copyfile(local_src, cached)
        return cached, f"local:{local_src}"

    fname = f"{vehicle_type}_tripdata_{year:04d}-{month:02d}.parquet"
    url = f"{TLC_BASE_URL}/{fname}"
    local_path = f"{CACHE_DIR}/{fname}"

    resp = requests.get(url, stream=True, timeout=60)
    if resp.status_code == 404:
        return None, url
    resp.raise_for_status()

    with open(local_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
    return local_path, url


def row_count(local_path):
    """Nombre de lignes lu depuis les métadonnées du footer Parquet — pas
    besoin de charger les données pour ça, c'est quasi instantané."""
    import pyarrow.parquet as pq
    return pq.ParquetFile(local_path).metadata.num_rows


def ingest_month(client, vehicle_type, year, month, force=False):
    partition_dir = taxi_partition_dir(vehicle_type, year, month)

    if not force and is_ingested(client, partition_dir):
        log.info("SKIP %s %04d-%02d (déjà ingéré)", vehicle_type, year, month)
        return "skipped"

    local_path, source = get_month_file(vehicle_type, year, month)

    if local_path is None:
        log.info("Pas encore publié : %s %04d-%02d (%s)", vehicle_type, year, month, source)
        return "not_published"

    log.info("Ingestion %s %04d-%02d depuis %s...", vehicle_type, year, month, source)
    n_rows = row_count(local_path)
    ensure_dir(client, partition_dir)
    hdfs_path = f"{partition_dir}/{vehicle_type}_tripdata_{year:04d}-{month:02d}.parquet"
    client.upload(hdfs_path, local_path, overwrite=True)

    write_success_marker(client, partition_dir, {
        "vehicle_type": vehicle_type,
        "year": year,
        "month": month,
        "source": source,
        "row_count": n_rows,
    })
    log.info("OK %s %04d-%02d : %d lignes (source: %s)", vehicle_type, year, month, n_rows, source)

    os.remove(local_path)
    return "ingested"


def main():
    parser = argparse.ArgumentParser(description="Ingestion Bronze des données TLC (local d'abord, réseau en secours).")
    parser.add_argument("--vehicle-type", choices=list(VEHICLE_START) + ["all"], default="all")
    parser.add_argument("--years", type=str, default=",".join(str(y) for y in DEFAULT_YEARS),
                         help="années à traiter, séparées par des virgules")
    parser.add_argument("--start-month", type=int, default=DEFAULT_START_MONTH)
    parser.add_argument("--end-month", type=int, default=DEFAULT_END_MONTH)
    parser.add_argument("--force", action="store_true", help="ré-ingère même si déjà présent")
    parser.add_argument("--max-months", type=int, default=None, help="limite le nb de mois traités (tests rapides)")
    args = parser.parse_args()

    years = [int(y.strip()) for y in args.years.split(",") if y.strip()]

    client = get_client()
    vehicle_types = list(VEHICLE_START) if args.vehicle_type == "all" else [args.vehicle_type]

    log.info("Source locale : %s (%s)", LOCAL_SOURCE_DIR,
              "montée" if os.path.isdir(LOCAL_SOURCE_DIR) else "absente, réseau utilisé pour tout")

    stats = {"ingested": 0, "skipped": 0, "not_published": 0}
    processed = 0

    for vt in vehicle_types:
        sy, sm = VEHICLE_START[vt]
        for year in years:
            for month in month_range(args.start_month, args.end_month):
                if (year, month) < (sy, sm):
                    continue  # avant la création du service, on ne tente même pas
                result = ingest_month(client, vt, year, month, force=args.force)
                stats[result] = stats.get(result, 0) + 1
                processed += 1
                if args.max_months and processed >= args.max_months:
                    log.info("Limite --max-months=%d atteinte, arrêt.", args.max_months)
                    _print_summary(stats)
                    return

    _print_summary(stats)


def _print_summary(stats):
    log.info("Résumé : %s", stats)


if __name__ == "__main__":
    sys.exit(main())
