import pandas as pd
import numpy as np
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RAW_PATH    = '/opt/airflow/data/raw/'
OUTPUT_PATH = '/opt/airflow/data/transformed/'

def transform():
    logger.info("Mulai transform data")
    os.makedirs(OUTPUT_PATH, exist_ok=True)

    # load semua raw data
    logger.info("Load raw parquet files")
    orders = pd.read_parquet(RAW_PATH + 'orders.parquet')
    items = pd.read_parquet(RAW_PATH + 'items.parquet')
    sellers = pd.read_parquet(RAW_PATH + 'sellers.parquet')
    customers = pd.read_parquet(RAW_PATH + 'customers.parquet')
    reviews = pd.read_parquet(RAW_PATH + 'reviews.parquet')
    products = pd.read_parquet(RAW_PATH + 'products.parquet')
    cat_trans = pd.read_parquet(RAW_PATH + 'cat_trans.parquet')

    # parse timestamp
    logger.info("Parse timestamps")
    ts_cols = [
        'order_purchase_timestamp',
        'order_approved_at',
        'order_delivered_carrier_date',
        'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]
    for col in ts_cols:
        orders[col] = pd.to_datetime(orders[col])

    # filter delivered only
    logger.info("Filter delivered orders")
    delivered = orders[orders['order_status'] == 'delivered'].copy()
    logger.info(f"Delivered orders: {len(delivered):,}")

    # derived columns
    logger.info("Hitung derived columns")

    delivered['t1_approval_h'] = (delivered['order_approved_at'] - delivered['order_purchase_timestamp']).dt.total_seconds() / 3600
    delivered['t2_processing_h'] = (delivered['order_delivered_carrier_date'] - delivered['order_approved_at']).dt.total_seconds() / 3600
    delivered['t3_shipping_h'] = (delivered['order_delivered_customer_date'] - delivered['order_delivered_carrier_date']).dt.total_seconds() / 3600

    delivered['t1_approval_h'] = delivered['t1_approval_h'].clip(lower=0)
    delivered['t2_processing_h'] = delivered['t2_processing_h'].clip(lower=0)
    delivered['t3_shipping_h'] = delivered['t3_shipping_h'].clip(lower=0)

    delivered['delivery_days'] = (delivered['order_delivered_customer_date'] - delivered['order_purchase_timestamp']).dt.days
    delivered['estimated_days'] = (delivered['order_estimated_delivery_date'] - delivered['order_purchase_timestamp']).dt.days
    delivered['days_late'] = (delivered['order_delivered_customer_date'] - delivered['order_estimated_delivery_date']).dt.days

    delivered['is_late'] = (delivered['order_delivered_customer_date'] > delivered['order_estimated_delivery_date']).astype(int)
    delivered['actual_vs_est_ratio'] = delivered['delivery_days'] / delivered['estimated_days'].replace(0, np.nan)

    delivered['purchase_month'] = delivered['order_purchase_timestamp'].dt.month.astype(int)
    delivered['purchase_dayofweek'] = delivered['order_purchase_timestamp'].dt.dayofweek.astype(int)
    delivered['purchase_hour'] = delivered['order_purchase_timestamp'].dt.hour.astype(int)

    # filter outlier yang terbukti error sistem
    logger.info("Filter outlier error sistem...")
    before = len(delivered)

    # 2 order approval delay 741 jam, system error platform Jan-Feb 2018
    delivered = delivered[delivered['t1_approval_h'] <= 700]

    # 40 order shipping ekstrem, batch update sistem 19 Sep 2017
    delivered = delivered[delivered['t3_shipping_h'] <= 3000]

    after = len(delivered)
    logger.info(f"Outlier dikeluarkan: {before - after} rows → sisa {after:,} rows")

    # siapkan dim_sellers
    logger.info("Buat dim_sellers")
    dim_sellers = sellers[['seller_id','seller_zip_code_prefix','seller_city','seller_state']].drop_duplicates()

    # siapkan dim_customers
    logger.info("Buat dim_customers")
    dim_customers = customers[['customer_id','customer_unique_id','customer_zip_code_prefix','customer_city','customer_state']].drop_duplicates()

    # siapkan dim_products
    logger.info("Buat dim_products")
    products['product_category_name'] = products['product_category_name'].fillna('unknown')
    dim_products = products.merge(cat_trans, on='product_category_name', how='left')
    dim_products['product_category_name_en'] = dim_products['product_category_name_english'].fillna(dim_products['product_category_name'])
    dim_products = dim_products[[
        'product_id', 'product_category_name', 'product_category_name_en',
        'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm'
    ]].drop_duplicates()

    # siapkan fact_deliveries
    logger.info("Buat fact_deliveries, join semua tabel")

    items_agg = items.groupby('order_id').agg(
        seller_id = ('seller_id', 'first'),
        product_id = ('product_id', 'first'),
        price = ('price', 'sum'),
        freight_value = ('freight_value', 'sum')
    ).reset_index()

    # seller cohort, kapan seller pertama kali punya order
    seller_first_ts = items.merge(
        delivered[['order_id','order_purchase_timestamp']], on='order_id', how='left'
    ).groupby('seller_id')['order_purchase_timestamp'].min().reset_index()
    seller_first_ts['seller_cohort'] = pd.to_datetime(
        seller_first_ts['order_purchase_timestamp']
    ).dt.to_period('Q').astype(str)
    seller_cohort_map = seller_first_ts.set_index('seller_id')['seller_cohort']
    items_agg['seller_cohort'] = items_agg['seller_id'].map(seller_cohort_map).fillna('unknown')

    fact = delivered.merge(items_agg, on='order_id', how='left')
    fact = fact.merge(customers[['customer_id','customer_state','customer_city']], on='customer_id', how='left')
    fact = fact.merge(sellers[['seller_id','seller_state','seller_city']], on='seller_id', how='left')
    fact = fact.merge(dim_products[['product_id','product_category_name','product_category_name_en']], on='product_id', how='left')
    fact = fact.merge(reviews[['order_id','review_score']], on='order_id', how='left')

    fact['product_category_name'] = fact['product_category_name'].fillna('unknown')
    fact['product_category_name_en'] = fact['product_category_name_en'].fillna('unknown')
    fact['review_score'] = fact['review_score'].fillna(0).astype(float)

    # freight bucket
    freight_quantiles = fact['freight_value'].quantile([0.2, 0.4, 0.6, 0.8]).values
    fact['freight_bucket'] = pd.cut(
        fact['freight_value'],
        bins=[-float('inf'), freight_quantiles[0], freight_quantiles[1], freight_quantiles[2], freight_quantiles[3], float('inf')],
        labels=['Sangat murah', 'Murah', 'Sedang', 'Mahal', 'Sangat mahal']
    ).astype(str)

    fact_deliveries = fact[[
        'order_id', 'customer_id', 'seller_id', 'product_id',
        'order_purchase_timestamp', 'order_approved_at',
        'order_delivered_carrier_date', 'order_delivered_customer_date',
        'order_estimated_delivery_date',
        't1_approval_h', 't2_processing_h', 't3_shipping_h',
        'delivery_days', 'estimated_days', 'days_late', 'actual_vs_est_ratio',
        'is_late', 'purchase_month', 'purchase_dayofweek', 'purchase_hour',
        'customer_state', 'customer_city',
        'seller_state', 'seller_city',
        'seller_cohort', 'freight_bucket',
        'product_category_name', 'product_category_name_en',
        'price', 'freight_value', 'review_score'
    ]]

    # siapkan aggregated tables
    logger.info("Buat aggregated tables")

    # agg_seller_performance, hanya seller aktif (>=20 orders)
    agg_seller_all = fact_deliveries.groupby('seller_id').agg(
        seller_state = ('seller_state', 'first'),
        seller_city = ('seller_city', 'first'),
        seller_cohort = ('seller_cohort', 'first'),
        total_orders = ('order_id', 'nunique'),
        late_orders = ('is_late', 'sum'),
        late_rate = ('is_late', 'mean'),
        avg_t2_processing_h = ('t2_processing_h', 'mean'),
        avg_delivery_days = ('delivery_days', 'mean'),
        total_revenue = ('price', 'sum')
    ).reset_index()

    # is_chronic= harus >= 20 orders DAN > 20% late rate
    agg_seller_all['is_chronic'] = (
        (agg_seller_all['late_rate'] > 0.2) &
        (agg_seller_all['total_orders'] >= 20)
    ).astype(int)

    # seller early warning: < 20 orders tapi 100% late
    agg_seller_all['is_new_problem'] = (
        (agg_seller_all['total_orders'] < 20) &
        (agg_seller_all['late_rate'] == 1.0)
    ).astype(int)

    # filter active seller untuk performance table
    agg_seller = agg_seller_all[agg_seller_all['total_orders'] >= 20].copy()

    # agg_regional_stats
    agg_regional = fact_deliveries.groupby('customer_state').agg(
        total_orders = ('order_id', 'count'),
        late_orders = ('is_late', 'sum'),
        late_rate = ('is_late', 'mean'),
        avg_delivery_days = ('delivery_days', 'mean'),
        avg_t3_shipping_h = ('t3_shipping_h', 'mean')
    ).reset_index()

    # agg_monthly_trend, pakai year_month bukan hanya bulan
    year_month_series = pd.to_datetime(
        fact_deliveries['order_purchase_timestamp']
    ).dt.to_period('M').astype(str)

    agg_monthly_df = fact_deliveries.copy()
    agg_monthly_df['year_month'] = year_month_series
    agg_monthly = agg_monthly_df.groupby('year_month').agg(
        total_orders = ('order_id', 'count'),
        late_orders = ('is_late', 'sum'),
        late_rate = ('is_late', 'mean'),
        avg_delivery_days = ('delivery_days', 'mean')
    ).reset_index()
    agg_monthly = agg_monthly.sort_values('year_month').reset_index(drop=True)

    # agg_cohort_performance
    agg_cohort = fact_deliveries.groupby('seller_cohort').agg(
        seller_count = ('seller_id', 'nunique'),
        total_orders = ('order_id', 'count'),
        late_orders = ('is_late', 'sum'),
        late_rate = ('is_late', 'mean'),
        avg_delivery_days = ('delivery_days', 'mean')
    ).reset_index()
    agg_cohort = agg_cohort.sort_values('seller_cohort').reset_index(drop=True)

    # kpi_summary
    ontime_rate = (~fact_deliveries['is_late'].astype(bool)).mean() * 100
    breach_rate = (fact_deliveries['days_late'].fillna(0) > 3).mean() * 100
    sla_mean = fact_deliveries['actual_vs_est_ratio'].dropna().mean()

    # fact_deliveries sudah punya seller_id, tidak perlu merge lagi
    seller_stats  = fact_deliveries.groupby('seller_id').agg(
        total_orders = ('order_id','nunique'),
        late_rate = ('is_late','mean')
    ).reset_index()
    seller_stats = fact_deliveries.groupby('seller_id').agg(
        total_orders = ('order_id','nunique'),
        late_rate = ('is_late','mean')
    ).reset_index()
    hv_sellers = seller_stats[seller_stats['total_orders'] >= 20]
    avg_late_seller = hv_sellers['late_rate'].mean() * 100

    t2_comply = (fact_deliveries['t2_processing_h'].dropna() <= 48).mean() * 100

    kpi_summary = pd.DataFrame([
        {
            'kpi_name'       : 'On-Time Delivery Rate',
            'actual_value'   : round(ontime_rate, 2),
            'target_value'   : 95.0,
            'target_operator': '>',
            'status'         : '✅' if ontime_rate >= 95 else '❌',
            'unit'           : '%'
        },
        {
            'kpi_name'       : 'Customer Tolerance Breach Rate',
            'actual_value'   : round(breach_rate, 2),
            'target_value'   : 2.0,
            'target_operator': '<',
            'status'         : '✅' if breach_rate <= 2 else '❌',
            'unit'           : '%'
        },
        {
            'kpi_name'       : 'SLA Accuracy Rate',
            'actual_value'   : round(sla_mean, 3),
            'target_value'   : 0.8,
            'target_operator': '>=',
            'status'         : '✅' if sla_mean >= 0.8 else '❌',
            'unit'           : 'ratio'
        },
        {
            'kpi_name'       : 'Late Rate per Seller (avg aktif)',
            'actual_value'   : round(avg_late_seller, 2),
            'target_value'   : 5.0,
            'target_operator': '<',
            'status'         : '✅' if avg_late_seller <= 5 else '❌',
            'unit'           : '%'
        },
        {
            'kpi_name'       : 'Processing Time Compliance',
            'actual_value'   : round(t2_comply, 2),
            'target_value'   : 80.0,
            'target_operator': '>',
            'status'         : '✅' if t2_comply >= 80 else '❌',
            'unit'           : '%'
        },
    ])

    # fact_late_risk — hasil prediksi XGBoost
    logger.info("Buat fact_late_risk, hasil prediksi ML")
    from xgboost import XGBClassifier
    from sklearn.preprocessing import LabelEncoder

    features_ml = ['customer_state', 'seller_state', 'purchase_month', 'purchase_dayofweek', 'purchase_hour', 'freight_bucket', 'seller_cohort']

    df_ml = fact_deliveries[features_ml + ['order_id', 'is_late']].dropna().copy()

    le_dict = {}
    X_all = df_ml[features_ml].copy()
    for col in ['customer_state', 'seller_state', 'freight_bucket', 'seller_cohort']:
        le = LabelEncoder()
        X_all[col] = le.fit_transform(X_all[col].astype(str))
        le_dict[col] = le

    y_all = df_ml['is_late'].astype(int)

    scale_pos_weight = (y_all == 0).sum() / (y_all == 1).sum()
    xgb_model = XGBClassifier(
        n_estimators=100,
        max_depth=6,
        learning_rate=0.1,
        scale_pos_weight=scale_pos_weight,
        random_state=42,
        eval_metric='auc',
        verbosity=0
    )
    xgb_model.fit(X_all, y_all)

    df_ml['late_probability'] = xgb_model.predict_proba(X_all)[:, 1].round(4)
    df_ml['late_prediction'] = xgb_model.predict(X_all).astype(int)
    df_ml['risk_level'] = pd.cut(
        df_ml['late_probability'],
        bins=[0, 0.3, 0.6, 1.0],
        labels=['Low Risk', 'Medium Risk', 'High Risk']
    ).astype(str)

    fact_late_risk = df_ml[[
        'order_id', 'customer_state', 'seller_state', 'seller_cohort',
        'purchase_month', 'freight_bucket',
        'late_probability', 'late_prediction', 'risk_level', 'is_late'
    ]].copy()

    # simpan semua ke parquet
    logger.info("Simpan hasil transform ke parquet")
    fact_deliveries.to_parquet(OUTPUT_PATH + 'fact_deliveries.parquet', index=False)
    dim_sellers.to_parquet(OUTPUT_PATH + 'dim_sellers.parquet', index=False)
    dim_customers.to_parquet(OUTPUT_PATH + 'dim_customers.parquet', index=False)
    dim_products.to_parquet(OUTPUT_PATH + 'dim_products.parquet', index=False)
    agg_seller.to_parquet(OUTPUT_PATH + 'agg_seller_performance.parquet', index=False)
    agg_regional.to_parquet(OUTPUT_PATH + 'agg_regional_stats.parquet', index=False)
    agg_monthly.to_parquet(OUTPUT_PATH + 'agg_monthly_trend.parquet', index=False)
    agg_cohort.to_parquet(OUTPUT_PATH + 'agg_cohort_performance.parquet', index=False)
    kpi_summary.to_parquet(OUTPUT_PATH + 'kpi_summary.parquet', index=False)
    fact_late_risk.to_parquet(OUTPUT_PATH + 'fact_late_risk.parquet', index=False)

    logger.info(f"✅ fact_deliveries        : {len(fact_deliveries):,} rows")
    logger.info(f"✅ dim_sellers            : {len(dim_sellers):,} rows")
    logger.info(f"✅ dim_customers          : {len(dim_customers):,} rows")
    logger.info(f"✅ dim_products           : {len(dim_products):,} rows")
    logger.info(f"✅ agg_seller_performance : {len(agg_seller):,} rows")
    logger.info(f"✅ agg_regional_stats     : {len(agg_regional):,} rows")
    logger.info(f"✅ agg_monthly_trend      : {len(agg_monthly):,} rows")
    logger.info(f"✅ agg_cohort_performance : {len(agg_cohort):,} rows")
    logger.info(f"✅ kpi_summary            : {len(kpi_summary):,} rows")
    logger.info(f"✅ fact_late_risk         : {len(fact_late_risk):,} rows")
    logger.info("Transform selesai")

if __name__ == '__main__':
    transform()