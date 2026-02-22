from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import pendulum # ใช้สำหรับจัดการเวลาไทย

# บอก Airflow ว่าไฟล์ main.py อยู่ที่ไหน
sys.path.append('/opt/airflow')

from main import run_pipeline

# 1. ตั้งค่าเขตเวลาเป็นประเทศไทย
local_tz = pendulum.timezone("Asia/Bangkok")

# 2. กำหนดค่าเริ่มต้นของงาน (ตัวแปรที่หายไปก่อนหน้านี้)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2026, 2, 22, tz=local_tz), # เริ่มวันนี้เวลาไทย
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 3. สร้าง DAG
with DAG(
    'crypto_elt_auto_run',
    default_args=default_args,
    # รันตอน 05:00 และ 17:00 ของทุกวันตามเวลาไทย
    schedule_interval='0 5,17 * * *', 
    catchup=False,
    tags=['crypto', 'elt']
) as dag:

    task_run_elt = PythonOperator(
        task_id='run_main_elt_script',
        python_callable=run_pipeline
    )