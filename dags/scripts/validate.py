import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TRANSFORMED_PATH = '/opt/airflow/data/transformed/'

def validate():
    logger.info("Mulai validasi data hasil transform")

    checks_passed = 0
    checks_failed = 0

    def check(condition, message):
        nonlocal checks_passed, checks_failed
        if condition:
            logger.info(f"PASS — {message}")
            checks_passed += 1
        else:
            logger.error(f"FAIL — {message}")
            checks_failed += 1

    # load semua transformed data
    fact = pd.read_parquet(TRANSFORMED_PATH + 'fact_deliveries.parquet')
    sellers = pd.read_parquet(TRANSFORMED_PATH + 'dim_sellers.parquet')
    customers = pd.read_parquet(TRANSFORMED_PATH + 'dim_customers.parquet')
    products = pd.read_parquet(TRANSFORMED_PATH + 'dim_products.parquet')
    agg_s = pd.read_parquet(TRANSFORMED_PATH + 'agg_seller_performance.parquet')
    agg_r = pd.read_parquet(TRANSFORMED_PATH + 'agg_regional_stats.parquet')
    agg_m = pd.read_parquet(TRANSFORMED_PATH + 'agg_monthly_trend.parquet')
    agg_c = pd.read_parquet(TRANSFORMED_PATH + 'agg_cohort_performance.parquet')
    kpi = pd.read_parquet(TRANSFORMED_PATH + 'kpi_summary.parquet')
    late_risk = pd.read_parquet(TRANSFORMED_PATH + 'fact_late_risk.parquet')

    # validasi row count
    check(len(fact) > 90000, f"fact_deliveries punya {len(fact):,} rows (ekspektasi >90.000)")
    check(len(fact) < 97500, f"Outlier sudah difilter — {len(fact):,} rows (seharusnya <97.500)")
    check(len(sellers) > 3000, f"dim_sellers punya {len(sellers):,} rows")
    check(len(customers) > 90000, f"dim_customers punya {len(customers):,} rows")
    check(len(products) > 30000, f"dim_products punya {len(products):,} rows")

    # validasi outlier sudah difilter
    check((fact['t1_approval_h'].dropna() <= 700).all(),
          "Tidak ada approval delay ekstrem (>700 jam) — system error sudah dikeluarkan")
    check((fact['t3_shipping_h'].dropna() <= 3000).all(),
          "Tidak ada shipping ekstrem (>3000 jam) — batch update sistem sudah dikeluarkan")

    # validasi kolom kritis tidak null
    check(fact['order_id'].isnull().sum() == 0, "order_id tidak ada null")
    check(fact['is_late'].isnull().sum() == 0, "is_late tidak ada null")
    check(fact['customer_state'].isnull().sum() == 0, "customer_state tidak ada null")
    check(fact['purchase_month'].isnull().sum() == 0, "purchase_month tidak ada null")
    check(fact['seller_cohort'].isnull().sum() == 0, "seller_cohort tidak ada null")
    check(fact['freight_bucket'].isnull().sum() == 0, "freight_bucket tidak ada null")

    # validasi nilai negatif sudah bersih
    check((fact['t1_approval_h'].dropna() >= 0).all(), "t1_approval_h tidak ada nilai negatif")
    check((fact['t2_processing_h'].dropna() >= 0).all(), "t2_processing_h tidak ada nilai negatif")
    check((fact['t3_shipping_h'].dropna() >= 0).all(), "t3_shipping_h tidak ada nilai negatif")

    # validasi is_late hanya 0 atau 1
    check(fact['is_late'].isin([0,1]).all(), "is_late hanya berisi 0 atau 1")

    # validasi late rate masuk akal (lebih ketat)
    overall_late_rate = fact['is_late'].mean()
    check(0.06 <= overall_late_rate <= 0.12,
          f"Late rate overall {overall_late_rate*100:.1f}% (ekspektasi 6-12%)")

    # validasi purchase_month valid
    check(fact['purchase_month'].between(1, 12).all(), "purchase_month antara 1-12")

    # validasi is_chronic logika benar
    if 'is_chronic' in agg_s.columns and 'total_orders' in agg_s.columns:
        wrong_chronic = agg_s[
            (agg_s['is_chronic'] == 1) & (agg_s['total_orders'] < 20)
        ]
        check(len(wrong_chronic) == 0,
              f"is_chronic hanya untuk seller aktif >=20 orders ({len(wrong_chronic)} yang salah ditandai)")

    # validasi agg tables tidak kosong 
    check(len(agg_s) > 0, f"agg_seller_performance tidak kosong ({len(agg_s):,} rows, hanya seller aktif)")
    check(len(agg_r) > 0, f"agg_regional_stats tidak kosong ({len(agg_r):,} rows)")
    check(len(agg_m) >= 12, f"agg_monthly_trend punya {len(agg_m)} year_month (ekspektasi >=12)")
    check(len(agg_c) > 0, f"agg_cohort_performance tidak kosong ({len(agg_c):,} rows)")
    check(len(kpi) == 5, f"kpi_summary punya 5 KPI ({len(kpi)} rows)")

    # validasi KPI values masuk akal
    if len(kpi) == 5:
        ontime = kpi[kpi['kpi_name'] == 'On-Time Delivery Rate']['actual_value'].values[0]
        check(80 <= ontime <= 100, f"On-Time Delivery Rate {ontime:.1f}% masuk akal (80-100%)")

    # validasi fact_late_risk
    check(len(late_risk) > 90000, f"fact_late_risk tidak kosong ({len(late_risk):,} rows)")
    check(late_risk['risk_level'].isin(['Low Risk', 'Medium Risk', 'High Risk']).all(), "risk_level hanya berisi Low/Medium/High Risk")
    check(late_risk['late_probability'].between(0, 1).all(), "late_probability antara 0 dan 1")

    # summary
    logger.info(f"\n{'='*50}")
    logger.info(f"Validasi selesai: {checks_passed} PASS, {checks_failed} FAIL")

    if checks_failed > 0:
        raise ValueError(f"{checks_failed} validasi gagal, Pipeline dihentikan")
    else:
        logger.info("Semua validasi lolos! Lanjut ke load.")

if __name__ == '__main__':
    validate()