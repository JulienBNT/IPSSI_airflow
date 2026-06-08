# TP2 - Premier DAG Airflow

DAG ETL simple avec 3 taches sequentielles : extraction, transformation et chargement.

---

## Prerequis

- Docker Desktop installe et demarre

---

## Lancer Airflow

1. Ouvrir un terminal dans le dossier `TP2`

2. Demarrer les conteneurs :

```
docker compose up -d
```

3. Attendre environ 1 minute que l'initialisation se termine, puis acceder à l'interface web :

```
http://localhost:8080
```

Identifiants par defaut :
- Login : `admin`
- Mot de passe : `admin`

4. Arreter les conteneurs :

```
docker compose down
```

---

## Structure du projet

```
TP2/
├── dags/
│   └── dag.py          # definition du DAG
├── logs/               # logs generes par Airflow
├── docker-compose.yml  # configuration des services
└── README.md
```

---

## Le DAG : `dag_etl`

Le DAG est declenche manuellement (`schedule_interval=None`). Il contient 3 taches qui s'executent dans l'ordre suivant :

```
extract_data  ->  transform_data  ->  load_data
```

### Taches

- `extract_data` : simule une extraction de donnees depuis une source
- `transform_data` : simule une transformation des donnees extraites
- `load_data` : simule un chargement des donnees transformees vers une destination

---

## Lancer le DAG manuellement

1. Dans l'interface web, aller dans l'onglet DAGs
2. Activer le DAG `dag_etl` en cliquant sur le bouton a gauche de son nom
3. Cliquer sur le bouton "Trigger DAG" (icone lecture) pour le lancer
4. Cliquer sur le DAG puis sur le run pour voir l'etat des taches

## Consulter les logs d'une tache

1. Dans la vue du run, cliquer sur une tache
2. Cliquer sur "Log" pour afficher les logs d'execution
