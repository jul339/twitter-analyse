# mspar

Chargement de tweets (NDJSON) dans TimescaleDB pour exploration (volume, période, langues, sources).

## Prérequis

- Docker et Docker Compose
- Données : fichiers `raw*.json` dans `raw/` (monté en `/data` dans le conteneur, une ligne = un tweet en JSON)

## Schéma

Table `tweets` : `ts`, `id_str`, `lang`, `source`, `screen_name`, `place`.

## Charger les données en base

1. Démarrer la base :

   ```bash
   docker compose up -d timescaledb
   ```

2. Lancer le chargement :

   ```bash
   docker compose run --rm --build backfill python scripts/ingest/backfill_parallel.py
   ```

   Variables d'environnement : `MAX_FILES` (limite de fichiers, 0 = tous), `BATCH_SIZE` (10000), `N_WORKERS`, `CHUNK_SIZE` (20).

   Pour tester sur 100 fichiers :  
   `MAX_FILES=100 docker compose run --rm backfill python scripts/ingest/backfill_parallel.py`

   Pour vider et réingérer :  
   `docker compose exec timescaledb psql -U mspar -d mspar -c "TRUNCATE tweets;"`

## Exploration des données (notebook)

TimescaleDB doit être démarrée (`docker compose up -d timescaledb`). À la racine du projet :

```bash
pip install -r requirements.txt
pip install jupyter
jupyter notebook notebooks/exploration.ipynb
```

Le notebook décrit volume, période, répartition temporelle, langues et sources.

## Connexion à la base

- Host: `localhost`, port: `5432`
- User / DB: `mspar`, password: `mspar`
- Exemple :  
  `docker compose exec timescaledb psql -U mspar -d mspar -c "SELECT count(*) FROM tweets;"`

## Autres scripts (hors Docker)

À la racine du projet, avec un `.env` configuré :

- `python scripts/test_bluesky.py` — test connexion Bluesky
- `python scripts/test_x_api.py` — test API X (Bearer token dans `.env`)
