import pandas as pd
import os
import io
import logging
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_PATH   = '/opt/airflow/data/'
OUTPUT_PATH = '/opt/airflow/data/raw/'
LAKE_PATH   = '/opt/airflow/data/lake/'

# Google Drive config
CREDENTIALS_PATH = '/opt/airflow/credentials/service_account.json'
FOLDER_ID        = '1JD5Nqljh39cq1Nem5hFsDm6VBSDZ8cpv'
SCOPES           = ['https://www.googleapis.com/auth/drive.readonly']

FILES = {
    'orders'    : 'orders.csv',
    'items'     : 'order_items.csv',
    'sellers'   : 'sellers.csv',
    'customers' : 'customers.csv',
    'reviews'   : 'order_reviews.csv',
    'products'  : 'products.csv',
    'cat_trans' : 'category_translation.csv',
}

def get_drive_service():
    creds = service_account.Credentials.from_service_account_file(
        CREDENTIALS_PATH, scopes=SCOPES
    )
    return build('drive', 'v3', credentials=creds)

def get_file_id(service, filename):
    results = service.files().list(
        q=f"name='{filename}' and '{FOLDER_ID}' in parents and trashed=false",
        fields='files(id, name)'
    ).execute()
    files = results.get('files', [])
    if not files:
        raise FileNotFoundError(f"File {filename} tidak ditemukan di Drive")
    return files[0]['id']

def download_from_drive(service, file_id):
    request = service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buffer.seek(0)
    return pd.read_csv(buffer)

def extract():
    logger.info("Mulai extract data dari Google Drive...")

    os.makedirs(OUTPUT_PATH, exist_ok=True)
    os.makedirs(LAKE_PATH, exist_ok=True)

    service = get_drive_service()
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    for name, filename in FILES.items():
        logger.info(f"Download {filename} dari Drive")

        # Download dari Google Drive
        file_id = get_file_id(service, filename)
        df = download_from_drive(service, file_id)

        # Simpan ke data lake dengan timestamp (versioning)
        lake_file = LAKE_PATH + f'{name}_{timestamp}.parquet'
        df.to_parquet(lake_file, index=False)
        logger.info(f"{filename} → data lake ({len(df):,} rows)")

        # Simpan ke raw untuk transform (sama seperti sebelumnya)
        out = OUTPUT_PATH + f'{name}.parquet'
        df.to_parquet(out, index=False)
        logger.info(f"{filename} → {name}.parquet ({len(df):,} rows)")

    logger.info("Extract selesai!")

if __name__ == '__main__':
    extract()