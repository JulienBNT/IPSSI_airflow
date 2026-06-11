CREATE TABLE IF NOT EXISTS weather_data (
    id              SERIAL PRIMARY KEY,
    city            VARCHAR(100)  NOT NULL,
    timestamp       TIMESTAMPTZ   NOT NULL,
    temperature_c   NUMERIC(5,2),
    humidity_pct    SMALLINT,
    wind_speed_kmh  NUMERIC(6,2),
    ingested_at     TIMESTAMPTZ   DEFAULT NOW(),
    CONSTRAINT uq_weather_city_ts UNIQUE (city, timestamp)
);

CREATE TABLE IF NOT EXISTS quality_anomalies (
    id          SERIAL PRIMARY KEY,
    city        VARCHAR(100)  NOT NULL,
    dag_run_id  VARCHAR(255)  NOT NULL,
    timestamp   VARCHAR(50)   NOT NULL,
    errors      TEXT[]        NOT NULL,
    detected_at TIMESTAMPTZ   DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ingestion_log (
    id              SERIAL PRIMARY KEY,
    dag_run_id      VARCHAR(255)  NOT NULL,
    execution_date  VARCHAR(50)   NOT NULL,
    cities_ok       SMALLINT,
    cities_ko       SMALLINT,
    status          VARCHAR(50),
    logged_at       TIMESTAMPTZ   DEFAULT NOW(),
    CONSTRAINT uq_ingestion_run UNIQUE (dag_run_id)
);
