from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd
import requests
from datetime import datetime, timedelta
import json
import time
import os
import urllib3
from google.cloud import storage

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BOT_FX = "https://gateway.api.bot.or.th/Stat-ExchangeRate/v2/MONTHLY_AVG_EXG_RATE/"
BOT_INT_AVG = "https://gateway.api.bot.or.th/LoanRate/v2/avg_loan_rate/"
CPI_API = "https://dataapi.moc.go.th/cpiu-indexes"

FX_API_KEY = "eyJvcmciOiI2NzM1NzgwZWM4YzFlYjAwMDEyYTM3NzEiLCJpZCI6ImZlZGY5YzI1ZWZlODRjMzNiMzQzNmUwNzQ4OTAxMTIyIiwiaCI6Im11cm11cjEyOCJ9"
INT_API_KEY = "eyJvcmciOiI2NzM1NzgwZWM4YzFlYjAwMDEyYTM3NzEiLCJpZCI6ImRiYzQyMzA5ZmRiZTQ3YzZiZWZjZDdkYTFmMGU4ZjNiIiwiaCI6Im11cm11cjEyOCJ9"

# ตัวแปรของ output_path ที่จะเซฟ
fx_output_path = "/home/airflow/gcs/data/fx_data.parquet"
cpi_output_path = "/home/airflow/gcs/data/cpi_data.parquet"
int_output_path = "/home/airflow/gcs/data/int_data.parquet"
cpi_int_output_path = "/home/airflow/gcs/data/cpi_int_data.parquet"


default_args = {
    'owner': 'anacha',
}


@task()
def get_fx_data(output_path):
    """ดึงข้อมูล FX จาก BOT"""

    def get_fx_res(API_URL, API_KEY, START_DATE, END_DATE):

        params = {
            "start_period": START_DATE,
            "end_period": END_DATE
        }
        headers = {
            "Authorization": API_KEY
        }
        
        try:
            response = requests.get(API_URL, params=params, headers=headers, verify=False, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            return None

    fx = get_fx_res(BOT_FX, FX_API_KEY, "2023-01", "2024-11")
    
    if not fx:
        raise Exception("Failed to fetch FX data from API")

    # สกัดข้อมูลจาก data_detail
    fx_detail = fx['result']['data']['data_detail']
    fx_df = pd.DataFrame(fx_detail)

    # ลบ column ที่ไม่ใช้
    fx_data = fx_df.drop(columns=['currency_name_eng'])
    
    # เซฟไฟล์ parquet
    fx_data.to_parquet(output_path, index=False)


@task()
def get_cpi_data(output_path):
    """ดึงข้อมูล CPI จาก MOC"""
    def get_cpiu_data(url, region_id, index_id, from_year, to_year):
        params = {
            'region_id': region_id,
            'index_id': index_id,
            'from_year': from_year,
            'to_year': to_year
        }
        
        try:
            response = requests.get(url, params=params, verify=False, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            return None

    CPI = get_cpiu_data(CPI_API, 5, '0000000000000000', 2023, 2024)
    
    if not CPI:
        raise Exception("Failed to fetch CPI data from API")

    # Clean ข้อมูล CPI
    CPI_df = pd.DataFrame(CPI)
    CPI_df = CPI_df.drop(columns=['index_id','index_description','region_id','region_name'])
    CPI_df['year_month'] = CPI_df['year'].astype(str) + '-' + CPI_df['month'].astype(str).str.zfill(2)
    CPI_clean = CPI_df.drop(columns=['year','month'])

    # เซฟไฟล์ parquet
    CPI_clean.to_parquet(output_path, index=False)


@task()
def get_int_data(output_path):
    """ดึงข้อมูลอัตราดอกเบี้ยจาก BOT"""
    
    def get_date_ranges(start_date, end_date):
        """แบ่งช่วงวันที่เป็นรายเดือน"""
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')

        ranges = []
        current_start = start

        while current_start <= end:
            # คำนวณวันสุดท้ายของเดือน
            if current_start.month == 12:
                next_month = current_start.replace(year=current_start.year + 1, month=1, day=1)
            else:
                next_month = current_start.replace(month=current_start.month + 1, day=1)

            month_end = next_month - timedelta(days=1)
            current_end = min(month_end, end)

            ranges.append((
                current_start.strftime('%Y-%m-%d'),
                current_end.strftime('%Y-%m-%d')
            ))

            current_start = next_month

        return ranges

    def fetch_data(api_url, api_key, start_date, end_date):
        """เรียก API ดึงข้อมูล"""
        params = {
            'start_period': start_date,
            'end_period': end_date
        }

        headers = {
            'Authorization': api_key
        }

        try:
            response = requests.get(api_url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {start_date} to {end_date}: {e}")
            return None

    def extract_details(response_data):
        """ดึง data_detail จาก response"""
        if response_data and 'result' in response_data:
            data = response_data.get('result', {}).get('data', {})
            return data.get('data_detail', [])
        return []

    def fetch_monthly_data(api_url, api_key, start_date, end_date, delay=1):
        """ดึงข้อมูลทีละเดือนและรวมกัน"""
        date_ranges = get_date_ranges(start_date, end_date)
        all_details = []

        for i, (start, end) in enumerate(date_ranges, 1):
            print(f"  [{i}/{len(date_ranges)}] {start} ถึง {end}...", end=' ')
            
            response_data = fetch_data(api_url, api_key, start, end)
            details = extract_details(response_data)

            if details:
                all_details.extend(details)
            else:
                print("ไม่พบข้อมูล")

            # หน่วงเวลาระหว่างการเรียก API
            if i < len(date_ranges):
                time.sleep(delay)

        return all_details

    data_details = fetch_monthly_data(BOT_INT_AVG, INT_API_KEY, "2023-01-01", "2024-11-30", delay=1)
    
    if not data_details:
        raise Exception("No interest rate data fetched from API")

    # Clean ข้อมูล int
    interest_df = pd.DataFrame(data_details)
    numeric_cols = ['mor', 'mlr', 'mrr', 'ceiling_rate', 'default_rate', 'creditcard_min', 'creditcard_max']
    
    for col in numeric_cols:
        if col in interest_df.columns:
            interest_df[col] = pd.to_numeric(interest_df[col], errors='coerce')

    # เพิ่ม year และ month จาก period
    interest_df['period'] = pd.to_datetime(interest_df['period'])
    interest_df['year'] = interest_df['period'].dt.year
    interest_df['month'] = interest_df['period'].dt.month

    # Group and aggregate
    available_cols = [col for col in numeric_cols if col in interest_df.columns]
    monthly_avg_interest_df = interest_df.groupby(['year', 'month'])[available_cols].mean().reset_index()

    monthly_avg_interest_df['year_month'] = (
        monthly_avg_interest_df['year'].astype(str) + '-' + 
        monthly_avg_interest_df['month'].astype(str).str.zfill(2)
    )

    monthly_avg_interest_df_clean = monthly_avg_interest_df.drop(columns=['year','month'])

    # เซฟไฟล์ parquet
    monthly_avg_interest_df_clean.to_parquet(output_path, index=False)


@task()
def merge_data(cpi_output_path, int_output_path, output_path):
    """Merge CPI และ Interest Rate data"""
    # อ่านจากไฟล์
    cpi_clean = pd.read_parquet(cpi_output_path)
    int_clean = pd.read_parquet(int_output_path)
    
    print(f"CPI Shape: {cpi_clean.shape}")
    print(f"Interest Shape: {int_clean.shape}")

    # merge 2 DataFrame
    cpi_int_df = cpi_clean.merge(int_clean, how="left", left_on="year_month", right_on="year_month")
    
    cpi_int_df.columns = ["base_year", "price_index", "mon", "yoy", "aoa", "year_month", "mor", "mlr", "mrr",
                          "ceiling_rate", "default_rate", "creditcard_min", "creditcard_max"]

    # save ไฟล์ Parquet
    cpi_int_df.to_parquet(output_path, index=False)


@dag(default_args=default_args, schedule_interval="@once", start_date=days_ago(1), tags=["workshop"])
def FX_Policy_CPI_Pipeline():
    """
    # FX_Policy_CPI_Pipeline
    ETL pipeline for FX, CPI, and Interest Rate data
    """
    t1 = get_fx_data(output_path=fx_output_path)
    t2 = get_cpi_data(output_path=cpi_output_path)
    t3 = get_int_data(output_path=int_output_path)
    
    t4 = merge_data(
        cpi_output_path=cpi_output_path,
        int_output_path=int_output_path,
        output_path=cpi_int_output_path
    )

    t5 = GCSToBigQueryOperator(
        task_id="load_fx_to_bigquery",
        bucket="us-central1-fxpipeline-908e3388-bucket",
        source_objects=["data/fx_data.parquet"],
        source_format="PARQUET",
        destination_project_dataset_table="DM_PROD.fx_data",
        write_disposition="WRITE_TRUNCATE"
    )

    t6 = GCSToBigQueryOperator(
        task_id="load_cpi_int_to_bigquery",
        bucket="us-central1-fxpipeline-908e3388-bucket",
        source_objects=["data/cpi_int_data.parquet"],
        source_format="PARQUET",
        destination_project_dataset_table="DM_PROD.cpi_int_data",
        write_disposition="WRITE_TRUNCATE"
    )
    
    [t1, t2, t3] >> t4 >> [t5, t6]


FX_Policy_CPI_Pipeline()