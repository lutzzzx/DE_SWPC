import json
import logging
import os
import sys
import boto3
import psycopg2
import psycopg2.extras
from botocore.client import Config
from botocore.exceptions import ClientError

# Konfigurasi Logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("Silver_Loader")

# Konfigurasi Environment & Kredensial
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
MINIO_BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE", os.getenv("MINIO_BUCKET", "space-weather-bronze"))

DWH_HOST = os.getenv("DWH_HOST", "postgres")
DWH_PORT = int(os.getenv("DWH_PORT", "5432"))
DWH_DB = os.getenv("DWH_DB", os.getenv("POSTGRES_DB", "space_weather_dwh"))
DWH_USER = os.getenv("DWH_USER", os.getenv("POSTGRES_USER", "dwh_admin"))
DWH_PASSWORD = os.getenv("DWH_PASSWORD", os.getenv("POSTGRES_PASSWORD", "DwhSecure_2026!"))

# Inisialisasi klien S3 boto3 untuk MinIO.
def get_minio_client():
    endpoint = MINIO_ENDPOINT
    if not endpoint.startswith("http://") and not endpoint.startswith("https://"):
        endpoint = f"http://{endpoint}"

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

# Membuka koneksi ke PostgreSQL Data Warehouse.
def get_pg_connection():
    return psycopg2.connect(
        host=DWH_HOST,
        port=DWH_PORT,
        dbname=DWH_DB,
        user=DWH_USER,
        password=DWH_PASSWORD,
    )

# Mencari object JSON terbaru berdasarkan atribut LastModified.
def get_latest_object_key(s3_client, bucket: str, dataset_name: str) -> str:
    prefix = f"bronze/{dataset_name}/"
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = response.get("Contents", [])
    if not contents:
        raise FileNotFoundError(f"Tidak ada objek ditemukan di bucket '{bucket}' dengan prefix '{prefix}'")
    
    # Ambil object yang paling baru diperbarui
    latest = max(contents, key=lambda obj: obj["LastModified"])
    return latest["Key"]

# Mengunduh dan melakukan deserialisasi payload JSON dari MinIO.
def download_json(s3_client, bucket: str, object_key: str) -> list:
    obj = s3_client.get_object(Bucket=bucket, Key=object_key)
    raw_body = obj["Body"].read()
    data = json.loads(raw_body.decode("utf-8"))
    if isinstance(data, dict):
        return [data]
    return data

# Transformasi & insert ke silver.planetary_k_index.
def load_planetary_k_index(cur, records: list, object_key: str) -> int:
    rows = []
    for record in records:
        time_tag = record.get("time_tag")
        if not time_tag:
            continue
        rows.append((
            time_tag,
            record.get("kp_index"),
            record.get("estimated_kp"),
            record.get("kp") or record.get("kp_source") or record.get("source"),
            object_key,
        ))
    if not rows:
        return 0

    query = """
        INSERT INTO silver.planetary_k_index
            (time_tag, kp_index, estimated_kp, kp_source, source_object_key)
        VALUES %s
        ON CONFLICT (time_tag, source_object_key) DO NOTHING;
    """
    psycopg2.extras.execute_values(cur, query, rows, page_size=1000)
    return len(rows)

# Transformasi & insert ke silver.solar_wind_mag.
def load_solar_wind_mag(cur, records: list, object_key: str) -> int:
    rows = []
    for record in records:
        time_tag = record.get("time_tag")
        if not time_tag:
            continue
        rows.append((
            time_tag,
            record.get("bx_gsm"),
            record.get("by_gsm"),
            record.get("bz_gsm"),
            record.get("bt"),
            object_key,
        ))
    if not rows:
        return 0

    query = """
        INSERT INTO silver.solar_wind_mag
            (time_tag, bx_gsm, by_gsm, bz_gsm, bt, source_object_key)
        VALUES %s
        ON CONFLICT (time_tag, source_object_key) DO NOTHING;
    """
    psycopg2.extras.execute_values(cur, query, rows, page_size=1000)
    return len(rows)

# Transformasi & insert ke silver.solar_wind_plasma.
def load_solar_wind_plasma(cur, records: list, object_key: str) -> int:
    rows = []
    for record in records:
        time_tag = record.get("time_tag")
        if not time_tag:
            continue
        
        # Penanganan aliasing nama kolom RTSW (proton_* vs standard)
        density = record.get("proton_density") if record.get("proton_density") is not None else record.get("density")
        speed = record.get("proton_speed") if record.get("proton_speed") is not None else record.get("speed")
        temperature = record.get("proton_temperature") if record.get("proton_temperature") is not None else record.get("temperature")

        rows.append((
            time_tag,
            density,
            speed,
            temperature,
            object_key,
        ))
    if not rows:
        return 0

    query = """
        INSERT INTO silver.solar_wind_plasma
            (time_tag, density, speed, temperature, source_object_key)
        VALUES %s
        ON CONFLICT (time_tag, source_object_key) DO NOTHING;
    """
    psycopg2.extras.execute_values(cur, query, rows, page_size=1000)
    return len(rows)

# Transformasi & insert ke silver.goes_xrays.
def load_goes_xrays(cur, records: list, object_key: str) -> int:
    rows = []
    for record in records:
        time_tag = record.get("time_tag")
        if not time_tag:
            continue
        
        energy_band = record.get("energy") or record.get("energy_band")
        observed_flag = str(record.get("observed_flag")) if record.get("observed_flag") is not None else None

        rows.append((
            time_tag,
            energy_band,
            record.get("flux"),
            observed_flag,
            object_key,
        ))
    if not rows:
        return 0

    query = """
        INSERT INTO silver.goes_xrays
            (time_tag, energy_band, flux, observed_flag, source_object_key)
        VALUES %s
        ON CONFLICT (time_tag, energy_band, source_object_key) DO NOTHING;
    """
    psycopg2.extras.execute_values(cur, query, rows, page_size=1000)
    return len(rows)


DATASET_LOADERS = {
    "planetary_k_index": load_planetary_k_index,
    "solar_wind_mag": load_solar_wind_mag,
    "solar_wind_plasma": load_solar_wind_plasma,
    "goes_xrays": load_goes_xrays,
}

# Fungsi orkestrasi loading Bronze ke Silver.
def run_loader():
    logger.info("=== Memulai Siklus Loading: Bronze (MinIO) -> Silver (PostgreSQL) ===")
    s3_client = get_minio_client()
    conn = get_pg_connection()
    conn.autocommit = False

    summary = {}
    try:
        with conn.cursor() as cur:
            for dataset_name, loader_fn in DATASET_LOADERS.items():
                try:
                    object_key = get_latest_object_key(s3_client, MINIO_BUCKET_BRONZE, dataset_name)
                    records = download_json(s3_client, MINIO_BUCKET_BRONZE, object_key)
                    inserted_count = loader_fn(cur, records, object_key)
                    summary[dataset_name] = {
                        "status": "SUCCESS",
                        "records_processed": inserted_count,
                        "source_key": object_key
                    }
                    logger.info(f"[{dataset_name}] Sukses memproses {inserted_count:,} baris dari {object_key}")
                except Exception as ds_err:
                    logger.error(f"[{dataset_name}] Gagal diproses: {ds_err}")
                    summary[dataset_name] = {"status": "FAILED", "error": str(ds_err)}
                    raise

        # Commit transaksi jika seluruh dataset sukses diproses
        conn.commit()
        logger.info("Transaksi berhasil di-commit ke PostgreSQL.")
    except Exception as err:
        conn.rollback()
        logger.error(f"Transaksi dibatalkan (rollback) karena error: {err}")
        raise
    finally:
        conn.close()

    logger.info("=== Ringkasan Hasil Loading Silver ===")
    for ds, result in summary.items():
        if result["status"] == "SUCCESS":
            logger.info(f" - {ds:<20}: {result['status']} ({result['records_processed']:,} baris)")
        else:
            logger.error(f" - {ds:<20}: {result['status']} -> {result.get('error')}")

    logger.info("=== Siklus Loading Silver Selesai dengan Sukses ===")


if __name__ == "__main__":
    try:
        run_loader()
    except Exception as fatal_error:
        logger.fatal(f"Eksekusi loader gagal: {fatal_error}")
        sys.exit(1)