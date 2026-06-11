# TP5 - Industrialisation d'un pipeline Airflow Open-Meteo

## Description du pipeline

Pipeline Airflow complet qui recupere des donnees meteo depuis l'API Open-Meteo
pour plusieurs villes configurables, les archive en brut, les transforme, controle
leur qualite, charge uniquement les donnees valides dans PostgreSQL, trace les
anomalies et enregistre chaque execution dans un journal d'ingestion.

---

## Schema du workflow

```
Pour chaque ville (en parallele) :

  fetch_{ville}
      |
  transform_{ville}
      |
  branch_{ville}  <-- BranchPythonOperator
     /         \
load_{ville}  flag_anomaly_{ville}
     \         /
      \       /
    log_ingestion  (trigger_rule=ALL_DONE)
```

---

## Variables d'environnement utilisees

Toutes les variables sont definies dans le fichier `.env`.

| Variable               | Description                                      |
|------------------------|--------------------------------------------------|
| CITIES                 | Liste des villes separees par des virgules       |
| OPEN_METEO_URL         | URL de base de l'API Open-Meteo                  |
| DB_METEO_HOST          | Hote de la base de donnees meteo                 |
| DB_METEO_PORT          | Port PostgreSQL (defaut : 5432)                  |
| DB_METEO_USER          | Utilisateur PostgreSQL                           |
| DB_METEO_PASSWORD      | Mot de passe PostgreSQL                          |
| DB_METEO_NAME          | Nom de la base de donnees                        |
| FORCE_QUALITY_FAILURE  | `true` pour simuler une anomalie qualite         |
| FAILURE_CITY           | Ville sur laquelle injecter l'anomalie           |

Les variables Airflow natives (executor, connexion metadata DB, etc.) sont
definies directement dans le `docker-compose.yml`.

---

## Connexions Airflow utilisees

Aucune connexion Airflow declaree via l'interface. Les connexions a PostgreSQL
sont etablies directement avec `psycopg2` en lisant les variables d'environnement.
Ce choix simplifie le demarrage mais en production on utiliserait
`PostgresHook(postgres_conn_id="meteo_db")` avec la connexion enregistree dans
Airflow via la variable `AIRFLOW_CONN_METEO_DB`.

---

## Description des taches du DAG

| Tache               | Type                  | Role                                                   |
|---------------------|-----------------------|--------------------------------------------------------|
| fetch_{ville}       | PythonOperator        | Appel API Open-Meteo, archivage JSON brut sur disque   |
| transform_{ville}   | PythonOperator        | Extraction des champs utiles, renommage                |
| branch_{ville}      | BranchPythonOperator  | Controle qualite + decision de branchement             |
| load_{ville}        | PythonOperator        | INSERT idempotent dans weather_data                    |
| flag_anomaly_{ville}| PythonOperator        | INSERT dans quality_anomalies, chargement bloque       |
| log_ingestion       | PythonOperator        | Bilan d'execution dans ingestion_log                   |

---

## Strategie de robustesse

- `retries=3` et `retry_delay=1min` sur toutes les taches (principalement utile pour fetch)
- `execution_timeout=5min` pour eviter les taches bloquees
- `raise_for_status()` sur la reponse HTTP pour echouer proprement en cas d'erreur API
- `trigger_rule=ALL_DONE` sur `log_ingestion` pour garantir le bilan meme en cas d'anomalie

---

## Strategie d'idempotence

- Table `weather_data` : contrainte `UNIQUE(city, timestamp)` + `ON CONFLICT DO NOTHING`
- Table `ingestion_log` : contrainte `UNIQUE(dag_run_id)` + `ON CONFLICT DO NOTHING`
- Re-executer le DAG avec le meme `run_id` ne cree aucun doublon

---

## Controles qualite mis en place

| Champ         | Regle                              |
|---------------|------------------------------------|
| Presence      | Tous les champs doivent etre non null |
| temperature_c | Entre -50.0 et 60.0 degres Celsius |
| humidity_pct  | Entre 0 et 100 %                   |
| wind_speed_kmh| Entre 0.0 et 300.0 km/h            |

---

## Regle de branchement conditionnel

Le `BranchPythonOperator` `branch_{ville}` :
- retourne `load_{ville}` si tous les controles qualite passent
- retourne `flag_anomaly_{ville}` si au moins une erreur est detectee

La tache non choisie est marquee `SKIPPED` par Airflow.

---

## Description des logs produits

Chaque tache logue via le logger Python standard :
- `fetch` : URL appelee, reponse brute JSON, chemin d'archive
- `transform` : donnees preparees, avertissement si anomalie injectee
- `branch` : resultat du controle qualite et decision prise
- `load` : confirmation d'insertion ou message d'idempotence
- `flag_anomaly` : liste des erreurs detectees
- `log_ingestion` : bilan nombre de villes OK/KO et statut global

---

## Description des tables PostgreSQL

### weather_data
Stocke les donnees meteo valides chargees.

| Colonne        | Type          | Description                   |
|----------------|---------------|-------------------------------|
| id             | SERIAL        | Cle primaire                  |
| city           | VARCHAR(100)  | Nom de la ville               |
| timestamp      | TIMESTAMPTZ   | Horodatage de la mesure       |
| temperature_c  | NUMERIC(5,2)  | Temperature en degres Celsius |
| humidity_pct   | SMALLINT      | Humidite relative en %        |
| wind_speed_kmh | NUMERIC(6,2)  | Vitesse du vent en km/h       |
| ingested_at    | TIMESTAMPTZ   | Date d'ingestion              |

Contrainte : `UNIQUE(city, timestamp)` pour l'idempotence.

### quality_anomalies
Trace les enregistrements rejetes par le controle qualite.

| Colonne     | Type         | Description                    |
|-------------|--------------|--------------------------------|
| id          | SERIAL       | Cle primaire                   |
| city        | VARCHAR(100) | Ville concernee                |
| dag_run_id  | VARCHAR(255) | Identifiant du run Airflow     |
| timestamp   | VARCHAR(50)  | Horodatage de la mesure        |
| errors      | TEXT[]       | Liste des erreurs detectees    |
| detected_at | TIMESTAMPTZ  | Date de detection              |

### ingestion_log
Journal d'execution de chaque run du DAG.

| Colonne        | Type         | Description                         |
|----------------|--------------|-------------------------------------|
| id             | SERIAL       | Cle primaire                        |
| dag_run_id     | VARCHAR(255) | Identifiant unique du run           |
| execution_date | VARCHAR(50)  | Date d'execution                    |
| cities_ok      | SMALLINT     | Nombre de villes chargees avec succes |
| cities_ko      | SMALLINT     | Nombre de villes avec anomalie      |
| status         | VARCHAR(50)  | `success` ou `partial`              |
| logged_at      | TIMESTAMPTZ  | Date d'enregistrement du log        |

Contrainte : `UNIQUE(dag_run_id)` pour l'idempotence.

---

## Preuves d'execution

### Cas nominal

Run `manual__2026-06-10T09:11:05` — toutes les villes passent le controle qualite.

```
 city      | timestamp              | temperature_c | humidity_pct
-----------+------------------------+---------------+--------------
 Paris     | 2026-06-10 11:00:00+00 |         15.80 |           55
 Lyon      | 2026-06-10 11:00:00+00 |         17.40 |           53
 Marseille | 2026-06-10 11:00:00+00 |         22.00 |           39

ingestion_log :
 manual__2026-06-10T09:11:05 | cities_ok=3 | cities_ko=0 | status=success
```

### Cas anomalie qualite

`FORCE_QUALITY_FAILURE=true`, `FAILURE_CITY=Paris` — temperature injectee a 999.0 C.

Branchement : `load_paris` → SKIPPED, `flag_anomaly_paris` → SUCCESS.

```
-- weather_data : Paris absent du run (chargement bloque)
 city      | timestamp              | temperature_c
-----------+------------------------+---------------
 Lyon      | 2026-06-10 11:15:00+00 |         17.50
 Marseille | 2026-06-10 11:15:00+00 |         22.30

-- quality_anomalies
 city  | errors
-------+-----------------------------------------------------------
 Paris | {"temperature_c hors plage [-50.0, 60.0] : valeur=999.0"}

-- ingestion_log
 manual__2026-06-10T09:16:19 | cities_ok=2 | cities_ko=1 | status=partial
```

### Cas relance sans doublon (run_id=rerun_idempotence_test)

Relance sur une fenetre deja chargee. Lyon et Marseille ont le meme timestamp
API que le run precedent (11:15). Le log Airflow confirme :

```
[load.py:41] INFO - [Lyon] Ligne deja presente — aucun doublon cree (idempotence).
[load.py:41] INFO - [Marseille] Ligne deja presente — aucun doublon cree (idempotence).
```

Nombre de lignes par ville apres 3 runs (dont 1 relance) :

```
SELECT city, COUNT(*) FROM weather_data GROUP BY city;
   city    | count
-----------+-------
 Lyon      |     2   <- 2 timestamps differents, 0 doublon
 Marseille |     2   <- 2 timestamps differents, 0 doublon
 Paris     |     2   <- 2 timestamps differents, 0 doublon
```

---

## Limites du travail rendu

- Les coordonnees GPS des villes sont hardcodees dans `fetch.py`. Pour une nouvelle
  ville hors du dictionnaire, le pipeline echoue. Une amelioration serait d'interroger
  une API de geocodage.
- Les connexions PostgreSQL utilisent psycopg2 directement plutot que
  `PostgresHook` d'Airflow, ce qui court-circuite le systeme de connexions Airflow.
- Le `schedule_interval=None` necessite un declenchement manuel. En production,
  on utiliserait `@daily` ou une expression cron.
- Les fichiers archives JSON ne sont pas nettoyes automatiquement.
