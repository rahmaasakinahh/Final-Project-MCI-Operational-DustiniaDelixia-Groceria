import pandas as pd
import clickhouse_connect
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TRANSFORMED_PATH = '/opt/airflow/data/transformed/'

def get_client():
    return clickhouse_connect.get_client(
        host='clickhouse-server',
        port=8123,
        username='admin',
        password='dustinia2026'
    )

def load():
    logger.info("Mulai load data ke ClickHouse...")
    client = get_client()

    # buat database
    client.command('CREATE DATABASE IF NOT EXISTS dustinia_db')
    logger.info("Database dustinia_db siap")

    # jalankan DDL
    logger.info("Buat tabel dari DDL...")
    with open('/opt/airflow/sql/ddl_operational.sql', 'r') as f:
        ddl = f.read()

    # drop semua tabel lama termasuk yang baru
    tables = [
        'fact_deliveries', 'dim_sellers', 'dim_customers', 'dim_products',
        'agg_seller_performance', 'agg_regional_stats', 'agg_monthly_trend',
        'agg_cohort_performance', 'kpi_summary', 'fact_late_risk'
    ]
    for table in tables:
        client.command(f'DROP TABLE IF EXISTS dustinia_db.{table}')
    logger.info("Tabel lama dihapus")

    # buat tabel baru dari DDL
    for statement in ddl.split(';'):
        statement = statement.strip()
        if statement:
            client.command(statement)
    logger.info("Semua tabel siap")

    def load_table(table_name, parquet_file):
        df = pd.read_parquet(TRANSFORMED_PATH + parquet_file)

        # konversi zip/prefix ke string
        for col in df.columns:
            if 'zip' in col.lower() or 'prefix' in col.lower():
                df[col] = df[col].astype(str)

        client.insert_df(f'dustinia_db.{table_name}', df)
        logger.info(f"{table_name}: {len(df):,} rows")

    load_table('dim_sellers', 'dim_sellers.parquet')
    load_table('dim_customers', 'dim_customers.parquet')
    load_table('dim_products', 'dim_products.parquet')
    load_table('fact_deliveries', 'fact_deliveries.parquet')
    load_table('agg_seller_performance', 'agg_seller_performance.parquet')
    load_table('agg_regional_stats', 'agg_regional_stats.parquet')
    load_table('agg_monthly_trend', 'agg_monthly_trend.parquet')
    load_table('agg_cohort_performance', 'agg_cohort_performance.parquet')
    load_table('kpi_summary', 'kpi_summary.parquet')
    load_table('fact_late_risk', 'fact_late_risk.parquet')

    logger.info("Semua data berhasil dimuat ke ClickHouse")

if __name__ == '__main__':
    load()