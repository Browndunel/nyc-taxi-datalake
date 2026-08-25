# Datalake NYC Taxi × Météo — Bronze / Silver / Gold

Pipeline complet sur HDFS, calcul Spark en **vrai cluster**
(`spark://spark-master:7077`, jamais `local[*]`), reprenant et adaptant
l'infrastructure Hadoop/Spark déjà validée sur le TD_Spark.

## Démarrer

```
docker compose up -d --build
```

Puis ouvrir Jupyter (le token s'affiche dans `docker compose logs
pyspark-notebook`) sur http://localhost:8888, et suivre le notebook
`work/atelier_datalake_taxi_nyc.ipynb` cellule par cellule : chaque
cellule "À COMPLÉTER" a été remplie et lance elle-même le script
correspondant (`!python ...` pour l'ingestion, `!spark-submit --master
spark://spark-master:7077 ...` pour Silver/Gold), donc l'ordre d'exécution
du notebook = l'ordre du pipeline.

- Suivre le cluster Spark : http://localhost:8080
- Suivre HDFS (NameNode UI) : http://localhost:9870

## Architecture

```
Sources (TLC CloudFront, Open-Meteo)
        |
        v
[Bronze]  HDFS, format natif (Parquet), une partition par
          vehicle_type/year/month — convention détaillée dans
          ingestion/hdfs_paths.py. Idempotent (marker _SUCCESS).
        |
        v
[Silver]  Spark cluster — réconciliation GPS/LocationID, distinction
          NULL (colonne pas encore introduite) vs 0 (valeur réelle) pour
          les suppléments progressifs, jointure météo. HDFS, Parquet
          partitionné vehicle_type/year/month.
        |
        v
[Gold]    5 tables dédiées, une par question métier du sujet — voir
          gold/*.py. Lues en pandas depuis le notebook pour les visus.
```

## Ce qui a été volontairement adapté / choisi (à connaître avant de rendre)

- **Infra réutilisée du TD_Spark, allégée** : namenode/datanode (HDFS) +
  spark-master/spark-worker, en gardant seulement ce qui sert réellement.
  Kafka retiré (inutile ici), et surtout resourcemanager/nodemanager/
  historyserver (YARN) retirés aussi — ce projet tourne en Spark
  **standalone** (`spark://spark-master:7077`), jamais via YARN, donc ces
  3 conteneurs (images lourdes) n'avaient aucune utilité. Réseau rendu
  autonome (plus de dépendance à un réseau externe).
- **Réconciliation GPS -> LocationID** : via une grille pré-calculée
  (`ingestion/build_zone_grid.py`, point-in-polygon fait une seule fois
  hors Spark avec geopandas) plutôt qu'un point-in-polygon par ligne dans
  Spark — beaucoup plus rapide à l'échelle de millions de lignes, au prix
  d'une résolution de grille de 0.001° (~110 m), largement suffisante
  face à la taille des zones TLC.
- **NULL vs 0 pour les suppléments** : basé sur la présence RÉELLE de la
  colonne dans le fichier source de chaque mois (lu partition par
  partition, jamais un `mergeSchema` global qui aurait rendu les deux cas
  indiscernables) — pas sur une date supposée, donc robuste même si la
  documentation publique de la TLC est imprécise sur une date exacte.
  Voir le commentaire en tête de `silver/transform_silver.py`.
- **5e analyse Gold ajoutée** : le sujet liste 5 analyses dans son énoncé
  ("Voici les quatre analyses..." suivi de 5 points numérotés) mais ne
  fournissait que 4 cellules "À COMPLÉTER" dans le notebook original.
  Une cellule a été ajoutée pour la 5e (écart de prix selon la météo) —
  à signaler si besoin, ce n'est pas une erreur de ma part mais une
  incohérence du sujet fourni.
- **`--max-months` par défaut à 8** dans les cellules d'ingestion : pour
  un premier run de bout-en-bout rapide (validation du pipeline complet
  avant de lancer l'ingestion complète, potentiellement longue vu le
  volume TLC). À retirer pour ingérer tout l'historique.

## Utilisation du dossier `nyc_taxi_data` déjà fourni

Confirmé via le listing de ton dossier : structure `<year>/<month>/
<vehicle_type>_tripdata_<year>-<month>.parquet`, 4 années (2009, 2016,
2019, 2025), mois 01 à 06 chacune. Les types présents par année confirment
exactement les dates de démarrage utilisées dans `VEHICLE_START`
(`taxi_ingest.py`) : 2009 = yellow seul, 2016 = yellow/green/fhv (pas de
fhvhv, créé en 2019), 2019/01 = pas encore de fhvhv (apparaît pile à partir
de 2019/02), 2025 = les 4 types.

`taxi_ingest.py` lit désormais ce dossier **en priorité** (monté en
lecture seule sur `/local_source` via `NYC_TAXI_DATA_DIR` dans `.env`) et
ne retombe sur le téléchargement réseau (TLC CloudFront) que pour un
fichier absent du local. Le périmètre par défaut des scripts d'ingestion
(taxi ET météo) est calé sur exactement ce qui est fourni : les 4 années x
6 mois — pas la peine d'aller plus large tant que la démo ne le demande
pas.

**Avant de lancer `docker compose up`** : vérifie/édite le fichier `.env`
à la racine du projet — `NYC_TAXI_DATA_DIR` doit pointer vers ton dossier
réel (`C:/Users/lenovo/Desktop/nyc_taxi_data` par défaut, avec des `/`,
c'est ce qu'attend Docker Desktop sous Windows).

## Structure du dépôt

```
docker-compose.yml       cluster HDFS + Spark + notebook + ingestion
hadoop.env                config Hadoop (repris du TD_Spark)
docker/Dockerfile.notebook  image notebook + libs de visualisation
ingestion/                 scripts Bronze (taxi, météo, référentiel zones)
silver/                    job Spark Silver
gold/                      5 jobs Spark Gold (un par analyse)
work/                      volume partagé avec les conteneurs Spark/notebook
  atelier_datalake_taxi_nyc.ipynb   notebook complété
  pipeline/                copie des scripts, accessible depuis le notebook
```
