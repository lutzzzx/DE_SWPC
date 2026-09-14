import os
import json
import logging
from datetime import datetime, timezone
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

# Konfigurasi Logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("SWPC_Ingestion")

# Konfigurasi Environment & MinIO S3
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
MINIO_BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE", "space-weather-bronze")

# Daftar Endpoint Resmi NOAA SWPC
SWPC_ENDPOINTS = {
    "planetary_k_index": "https://services.swpc.noaa.gov/json/planetary_k_index_1m.json",
    "solar_wind_mag": "https://services.swpc.noaa.gov/json/rtsw/rtsw_mag_1m.json",
    "solar_wind_plasma": "https://services.swpc.noaa.gov/json/rtsw/rtsw_wind_1m.json",
    "goes_xrays": "https://services.swpc.noaa.gov/json/goes/primary/xrays-1-day.json",
}

# Inisialisasi klien S3 MinIO
def get_s3_client():
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


# Membuat session HTTP dengan retry logic otomatis
def create_requests_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

# Mengambil payload JSON mentah dari endpoint NOAA SWPC
def fetch_swpc_data(session: requests.Session, name: str, url: str) -> list:
    logger.info(f"Mengambil data [{name}] dari: {url}")
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            logger.warning(f"Respon [{name}] bukan berupa list. Tipe terdeteksi: {type(data)}")
            return [data]
        logger.info(f"Berhasil mengunduh [{name}]: {len(data):,} baris record diterima.")
        return data
    except requests.exceptions.RequestException as exc:
        logger.error(f"Gagal mengambil data [{name}] dari {url}: {exc}")
        raise

# Mengunggah raw data JSON ke MinIO Bronze
def upload_to_bronze(s3_client, dataset_name: str, raw_data: list, source_url: str) -> str:
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    timestamp_str = now.strftime("%Y%m%d_%H%M%S")
    object_key = f"bronze/{dataset_name}/{date_str}/{dataset_name}_{timestamp_str}.json"

    # Serialisasi ke byte payload JSON
    payload_bytes = json.dumps(raw_data, ensure_ascii=False, indent=2).encode("utf-8")
    payload_size_kb = len(payload_bytes) / 1024

    metadata = {
        "dataset_name": dataset_name,
        "source_url": source_url,
        "record_count": str(len(raw_data)),
        "ingested_at": now.isoformat(),
    }

    try:
        s3_client.put_object(
            Bucket=MINIO_BUCKET_BRONZE,
            Key=object_key,
            Body=payload_bytes,
            ContentType="application/json",
            Metadata=metadata
        )
        logger.info(
            f"Tersimpan di Bronze: s3://{MINIO_BUCKET_BRONZE}/{object_key} "
            f"({payload_size_kb:.2f} KB, {len(raw_data):,} record)"
        )
        return object_key
    except ClientError as err:
        logger.error(f"Gagal mengunggah {object_key} ke MinIO: {err}")
        raise

# Orkestrasi ingestion untuk seluruh dataset NOAA SWPC.
def run_ingestion():
    logger.info("Memulai Siklus Ingestion NOAA SWPC -> MinIO Bronze")
    s3_client = get_s3_client()
    session = create_requests_session()

    # Pastikan bucket bronze tersedia
    try:
        s3_client.head_bucket(Bucket=MINIO_BUCKET_BRONZE)
    except ClientError:
        logger.info(f"Bucket '{MINIO_BUCKET_BRONZE}' belum ada, membuat bucket baru...")
        s3_client.create_bucket(Bucket=MINIO_BUCKET_BRONZE)

    summary = {}
    for name, url in SWPC_ENDPOINTS.items():
        try:
            records = fetch_swpc_data(session, name, url)
            s3_key = upload_to_bronze(s3_client, name, records, url)
            summary[name] = {"status": "SUCCESS", "records": len(records), "s3_key": s3_key}
        except Exception as e:
            logger.error(f"Ingestion gagal untuk dataset [{name}]: {e}")
            summary[name] = {"status": "FAILED", "error": str(e)}

    logger.info("=== Ringkasan Ingestion Bronze ===")
    for ds, result in summary.items():
        status = result["status"]
        if status == "SUCCESS":
            logger.info(f" - {ds:<20}: {status} ({result['records']:,} record) -> {result['s3_key']}")
        else:
            logger.error(f" - {ds:<20}: {status} -> Error: {result.get('error')}")

    # Lempar error jika salah satu dataset gagal agar scheduler dapat mencatat status failure
    failed_datasets = [ds for ds, res in summary.items() if res["status"] != "SUCCESS"]
    if failed_datasets:
        raise RuntimeError(f"Ingestion gagal untuk dataset: {', '.join(failed_datasets)}")

    logger.info("=== Siklus Ingestion Selesai dengan Sukses ===")


if __name__ == "__main__":
    run_ingestion()