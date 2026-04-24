-- schema/ddl.sql
-- Tablas destino en el Postgres del docker-compose de Airflow.
-- Ejecutar una sola vez antes del primer run del DAG.
-- Idempotente: usa IF NOT EXISTS.

-- ---------------------------------------------------------------
-- regulations
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS regulations (
    id                SERIAL PRIMARY KEY,
    created_at        DATE,
    update_at         TIMESTAMP,
    is_active         BOOLEAN         DEFAULT TRUE,
    title             VARCHAR(65)     NOT NULL,
    gtype             VARCHAR(20),
    entity            VARCHAR(200)    NOT NULL,
    external_link     TEXT,
    rtype_id          INTEGER,
    summary           TEXT,
    classification_id INTEGER         NOT NULL,

    -- Idempotencia: evitar duplicados por (title, created_at, external_link)
    CONSTRAINT uq_regulations_title_date_link
        UNIQUE (title, created_at, external_link, entity)
);

CREATE INDEX IF NOT EXISTS idx_regulations_entity
    ON regulations (entity);

CREATE INDEX IF NOT EXISTS idx_regulations_created_at
    ON regulations (created_at DESC);

-- ---------------------------------------------------------------
-- regulations_component
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS regulations_component (
    id               SERIAL PRIMARY KEY,
    regulations_id   INTEGER NOT NULL REFERENCES regulations (id) ON DELETE CASCADE,
    components_id    INTEGER NOT NULL,

    CONSTRAINT uq_regulations_component
        UNIQUE (regulations_id, components_id)
);

CREATE INDEX IF NOT EXISTS idx_reg_component_regulation
    ON regulations_component (regulations_id);
