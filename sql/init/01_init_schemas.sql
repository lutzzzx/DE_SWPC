-- Inisialisasi skema Medallion Architecture
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- 1. Tabel Silver: Planetary K-Index
CREATE TABLE IF NOT EXISTS silver.planetary_k_index (
    id                BIGSERIAL PRIMARY KEY,
    time_tag          TIMESTAMPTZ NOT NULL,
    kp_index          NUMERIC(4,2),
    estimated_kp      NUMERIC(4,2),
    kp_source         TEXT,
    source_object_key TEXT NOT NULL,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_planetary_k_index UNIQUE (time_tag, source_object_key)
);

-- 2. Tabel Silver: Solar Wind Magnetic Field (RTSW Mag)
CREATE TABLE IF NOT EXISTS silver.solar_wind_mag (
    id                BIGSERIAL PRIMARY KEY,
    time_tag          TIMESTAMPTZ NOT NULL,
    bx_gsm            NUMERIC(10,4),
    by_gsm            NUMERIC(10,4),
    bz_gsm            NUMERIC(10,4),
    bt                NUMERIC(10,4),
    source_object_key TEXT NOT NULL,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_solar_wind_mag UNIQUE (time_tag, source_object_key)
);

-- 3. Tabel Silver: Solar Wind Plasma (RTSW Wind)
CREATE TABLE IF NOT EXISTS silver.solar_wind_plasma (
    id                BIGSERIAL PRIMARY KEY,
    time_tag          TIMESTAMPTZ NOT NULL,
    density           NUMERIC(10,4),
    speed             NUMERIC(10,4),
    temperature       NUMERIC(14,4),
    source_object_key TEXT NOT NULL,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_solar_wind_plasma UNIQUE (time_tag, source_object_key)
);

-- 4. Tabel Silver: GOES Primary X-Rays
CREATE TABLE IF NOT EXISTS silver.goes_xrays (
    id                BIGSERIAL PRIMARY KEY,
    time_tag          TIMESTAMPTZ NOT NULL,
    energy_band       TEXT,
    flux              NUMERIC(20,10),
    observed_flag     TEXT,
    source_object_key TEXT NOT NULL,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_goes_xrays UNIQUE (time_tag, energy_band, source_object_key)
);

-- Indeks Waktu untuk Akselerasi Range Query & Join
CREATE INDEX IF NOT EXISTS idx_kp_time_tag ON silver.planetary_k_index (time_tag);
CREATE INDEX IF NOT EXISTS idx_mag_time_tag ON silver.solar_wind_mag (time_tag);
CREATE INDEX IF NOT EXISTS idx_plasma_time_tag ON silver.solar_wind_plasma (time_tag);
CREATE INDEX IF NOT EXISTS idx_xrays_time_tag ON silver.goes_xrays (time_tag);