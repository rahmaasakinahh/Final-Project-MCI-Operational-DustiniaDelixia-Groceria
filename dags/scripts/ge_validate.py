import pandas as pd
import logging
import os
import glob
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TRANSFORMED_PATH = '/opt/airflow/data/transformed/'
REPORT_PATH = '/opt/airflow/data/ge_reports/'

def ge_validate():
    import great_expectations as ge

    os.makedirs(REPORT_PATH, exist_ok=True)

    # Auto-delete report lebih dari 4 file
    reports = sorted(glob.glob(REPORT_PATH + 'data_quality_report_*.html'))
    if len(reports) > 4:
        for old_report in reports[:-4]:
            os.remove(old_report)
            logger.info(f"Hapus report lama: {old_report}")

    logger.info("Mulai Great Expectations validation")

    fact = ge.read_parquet(TRANSFORMED_PATH + 'fact_deliveries.parquet')

    results = []

    r = fact.expect_column_values_to_not_be_null('order_id')
    results.append(('order_id tidak null', 'order_id', r))

    r = fact.expect_column_values_to_be_in_set('is_late', [0, 1])
    results.append(('is_late hanya 0 atau 1', 'is_late', r))

    r = fact.expect_column_mean_to_be_between('is_late', 0.06, 0.12)
    results.append(('Late rate 6-12%', 'is_late', r))

    r = fact.expect_column_values_to_be_between('purchase_month', 1, 12)
    results.append(('purchase_month antara 1-12', 'purchase_month', r))

    r = fact.expect_column_values_to_be_between('t1_approval_h', 0, 700, mostly=0.999)
    results.append(('Tidak ada approval delay ekstrem', 't1_approval_h', r))

    r = fact.expect_column_values_to_be_between('t3_shipping_h', 0, 3000, mostly=0.999)
    results.append(('Tidak ada shipping ekstrem', 't3_shipping_h', r))

    r = fact.expect_table_row_count_to_be_between(90000, 100000)
    results.append(('Row count 90K-100K', 'table', r))

    r = fact.expect_column_values_to_not_be_null('seller_cohort')
    results.append(('seller_cohort tidak null', 'seller_cohort', r))

    r = fact.expect_column_values_to_be_in_set(
        'freight_bucket',
        ['Sangat murah', 'Murah', 'Sedang', 'Mahal', 'Sangat mahal']
    )
    results.append(('freight_bucket valid', 'freight_bucket', r))

    passed = sum(1 for _, _, r in results if r['success'])
    failed = sum(1 for _, _, r in results if not r['success'])

    # Simpan per minggu
    week_label = datetime.now().strftime('%Y-W%W')
    report_file = REPORT_PATH + f'data_quality_report_{week_label}.html'

    html = f"""<html>
<head>
    <title>Data Quality Report - DustiniaDelixia</title>
    <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@400;500;600&display=swap" rel="stylesheet">
    <style>
        body {{ font-family: 'Poppins', sans-serif; padding: 20px; background: #f5f5f5; }}
        h1 {{ color: #333; }}
        .summary {{ background: white; padding: 15px; border-radius: 8px; margin-bottom: 20px; }}
        .pass {{ color: green; font-weight: bold; }}
        .fail {{ color: red; font-weight: bold; }}
        table {{ width: 100%; border-collapse: collapse; background: white; }}
        th {{ background: #4CAF50; color: white; padding: 10px; text-align: left; }}
        td {{ padding: 10px; border-bottom: 1px solid #ddd; }}
    </style>
</head>
<body>
    <h1>Data Quality Report - DustiniaDelixia Groceria</h1>
    <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    <div class="summary">
        <h2>Summary</h2>
        <p>Total checks: {len(results)}</p>
        <p class="pass">PASS: {passed}</p>
        <p class="fail">FAIL: {failed}</p>
    </div>
    <table>
        <tr><th>Check</th><th>Column</th><th>Status</th></tr>"""

    for name, col, r in results:
        status = 'PASS' if r['success'] else 'FAIL'
        color = 'pass' if r['success'] else 'fail'
        html += f"<tr><td>{name}</td><td>{col}</td><td class='{color}'>{status}</td></tr>"

    html += """</table>
    <h2 style="margin-top:30px;">Riwayat Run Minggu Ini</h2>
    <div id="runs"></div>
    </body></html>"""

    if os.path.exists(report_file):
        with open(report_file, 'r') as f:
            existing = f.read()
        # Sisipkan run baru setelah tag <body>
        new_run = f"""
        <div style="border: 1px solid #ddd; border-radius: 8px; margin-bottom: 20px; overflow: hidden;">
            <div style="background: #333; color: white; padding: 10px 15px; font-family: 'Poppins', sans-serif;">
                Run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} — 
                <span style="color: {'#4CAF50' if failed == 0 else '#f44336'}">
                    {passed} PASS, {failed} FAIL
                </span>
            </div>
            <table style="width:100%; border-collapse:collapse; background:white;">
                <tr><th style="background:#4CAF50;color:white;padding:10px;text-align:left;">Check</th>
                    <th style="background:#4CAF50;color:white;padding:10px;text-align:left;">Column</th>
                    <th style="background:#4CAF50;color:white;padding:10px;text-align:left;">Status</th></tr>
        """
        for name, col, r in results:
            status = 'PASS' if r['success'] else 'FAIL'
            color = 'green' if r['success'] else 'red'
            new_run += f"<tr><td style='padding:10px;border-bottom:1px solid #ddd'>{name}</td><td style='padding:10px;border-bottom:1px solid #ddd'>{col}</td><td style='padding:10px;border-bottom:1px solid #ddd;color:{color};font-weight:bold'>{status}</td></tr>"
        new_run += "</table></div>"

        updated = existing.replace('<div id="runs">', f'<div id="runs">{new_run}')
        with open(report_file, 'w') as f:
            f.write(updated)
    else:
        with open(report_file, 'w') as f:
            f.write(html)

    logger.info(f"GE Report: {passed} PASS, {failed} FAIL")
    logger.info(f"Report saved: {report_file}")

    if failed > 0:
        raise ValueError(f"{failed} data quality checks gagal!")

if __name__ == '__main__':
    ge_validate()