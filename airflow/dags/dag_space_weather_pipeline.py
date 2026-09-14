import os
import logging
from datetime import timedelta
import pendulum
import psycopg2

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator

# Inisialisasi Logger & Konfigurasi Failure Callback
logger = logging.getLogger("airflow.task")

def notify_failure(context):
    task_instance = context.get("task_instance")
    logical_date = context.get("logical_date", context.get("execution_date"))
    exception = context.get("exception")

    alert_msg = (
        f"\n======================================================\n"
        f"[ALERT DATA PIPELINE FAILURE]\n"
        f"DAG ID         : {task_instance.dag_id}\n"
        f"Task ID        : {task_instance.task_id}\n"
        f"Logical Date   : {logical_date}\n"
        f"Log URL        : {task_instance.log_url}\n"
        f"Exception Info : {exception}\n"
        f"======================================================"
    )
    logger.error(alert_msg)

    # Integrasi Opsional: Kirim payload ke Webhook (Discord / Slack / Teams) jika URL tersedia
    webhook_url = os.getenv("ALERT_WEBHOOK_URL")
    if webhook_url:
        try:
            import json
            import urllib.request

            payload = json.dumps({"content": alert_msg, "text": alert_msg}).encode("utf-8")
            req = urllib.request.Request(
                webhook_url,
                data=payload,
                headers={"User-Agent": "Airflow-Alerter", "Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                logger.info(f"Notifikasi webhook berhasil dikirim (Status: {resp.status})")
        except Exception as e:
            logger.warning(f"Gagal mengirim notifikasi ke webhook: {e}")


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": notify_failure,
}


@dag(
    dag_id="space_weather_pipeline",
    description="Pipeline Ingestion & Transformasi Cuaca Antariksa NOAA SWPC (Bronze -> Silver -> Gold)",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["space-weather", "medallion", "dbt", "minio", "on-premise"],
)
def space_weather_pipeline():

    # 1. Task: Ekstraksi Data NOAA SWPC -> MinIO Bronze
    extract_to_bronze = BashOperator(
        task_id="extract_to_bronze",
        bash_command="python /opt/airflow/scripts/extract_swpc.py",
    )

    # 2. Task: Loading & Parsing Bronze -> PostgreSQL Silver
    load_to_silver = BashOperator(
        task_id="load_to_silver",
        bash_command="python /opt/airflow/scripts/load_silver.py",
    )

    # 3. Task: Validasi Kualitas Data (Circuit Breaker)
    @task(task_id="validate_silver_freshness")
    def validate_silver_freshness():
        host = os.getenv("DWH_HOST", "postgres")
        port = int(os.getenv("DWH_PORT", "5432"))
        dbname = os.getenv("DWH_DB", "space_weather_dwh")
        user = os.getenv("DWH_USER", "dwh_admin")
        password = os.getenv("DWH_PASSWORD", "SpaceWeather_2026!")

        query = """
            SELECT
                (SELECT COUNT(*) FROM silver.planetary_k_index WHERE loaded_at > now() - interval '60 minutes'),
                (SELECT COUNT(*) FROM silver.solar_wind_mag WHERE loaded_at > now() - interval '60 minutes'),
                (SELECT COUNT(*) FROM silver.solar_wind_plasma WHERE loaded_at > now() - interval '60 minutes'),
                (SELECT COUNT(*) FROM silver.goes_xrays WHERE loaded_at > now() - interval '60 minutes')
        """

        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
        )

        try:
            with conn.cursor() as cur:
                cur.execute(query)
                counts = cur.fetchone()

            kp_count, mag_count, plasma_count, xrays_count = counts
            logger.info(
                f"[CIRCUIT BREAKER] Record 60 menit terakhir: "
                f"planetary_k_index={kp_count}, solar_wind_mag={mag_count}, "
                f"solar_wind_plasma={plasma_count}, goes_xrays={xrays_count}"
            )

            # Jika ada tabel yang kosong dalam 60 menit terakhir, hentikan pipeline
            if any(count == 0 for count in counts):
                raise AirflowFailException(
                    f"Circuit Breaker Triggered: Data Silver tidak mutakhir! Hasil hitung: {counts}"
                )
            logger.info("Validasi Silver Freshness Sukses: Semua tabel memiliki data mutakhir.")
        finally:
            conn.close()

    # 4. Task: Unduh Dependensi dbt (dbt_utils)
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="""
        export PATH="/home/airflow/.local/bin:$PATH"
        cd /opt/airflow/dbt/space_weather && \
        if [ ! -d "dbt_packages" ]; then
            dbt deps
        else
            echo "dbt_packages sudah terpasang, melewati dbt deps."
        fi
        """,
        env={"DBT_PROFILES_DIR": "/opt/airflow/dbt/space_weather"},
        append_env=True,
    )

    # 5. Task: Eksekusi Model Transformasi dbt (Staging & Marts)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="export PATH=\"/home/airflow/.local/bin:$PATH\" && cd /opt/airflow/dbt/space_weather && dbt run --fail-fast",
        env={"DBT_PROFILES_DIR": "/opt/airflow/dbt/space_weather"},
        append_env=True,
    )

    # 6. Task: Pengujian Kualitas Data dbt (Data Quality Tests)
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="export PATH=\"/home/airflow/.local/bin:$PATH\" && cd /opt/airflow/dbt/space_weather && dbt test",
        env={"DBT_PROFILES_DIR": "/opt/airflow/dbt/space_weather"},
        append_env=True,
    )


    # Definisi Dependensi Alur Task (Linear Pipeline)
    freshness_task = validate_silver_freshness()

    extract_to_bronze >> load_to_silver >> freshness_task >> dbt_deps >> dbt_run >> dbt_test


space_weather_pipeline()
