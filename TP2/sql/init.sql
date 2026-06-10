CREATE TABLE IF NOT EXISTS weather_data (
    id              SERIAL PRIMARY KEY,
    city            VARCHAR(100)  NOT NULL,
    timestamp       TIMESTAMPTZ   NOT NULL,
    temperature_c   NUMERIC(5,2),
    humidity_pct    SMALLINT,
    wind_speed_kmh  NUMERIC(6,2),
    ingested_at     TIMESTAMPTZ   DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ingestion_log (
    id              SERIAL PRIMARY KEY,
    dag_run_id      VARCHAR(255)  NOT NULL,
    execution_date  TIMESTAMPTZ   NOT NULL,
    cities_count    SMALLINT,
    rows_inserted   SMALLINT,
    status          VARCHAR(50),
    logged_at       TIMESTAMPTZ   DEFAULT NOW()
);
