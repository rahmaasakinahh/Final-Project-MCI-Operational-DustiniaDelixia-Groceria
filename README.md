---

# **DustiniaDelixia Groceria | Operational Analytics Pipeline**

---

> Seleksi Admin Lab MCI 2026 | Final Project | Operational Analyst

| Nama | NRP | Program Studi |
|---|---|---|
| Rahma Sakinah | 5025241222 | Teknik Informatika |

---

# **Architecture**

---

<img width="956" height="875" alt="image" src="https://github.com/user-attachments/assets/b1549041-b1d9-46b4-a4e5-4f10922bc2af" />

Dataset diambil dari Google Drive via Google Drive API, diproses secara berurutan oleh 5 task di Airflow (extract, transform, validate, ge_validate, load). Setelah data masuk ke ClickHouse, Metabase membacanya dan menampilkan hasilnya dalam bentuk dashboard operasional yang bercerita dari kondisi saat ini sampai prediksi ke depan.

---

# **Tech Stack**

---

| Tool | Versi | Fungsi |
|---|---|---|
| Apache Airflow | 2.9.1 | Orkestrasi pipeline otomatis |
| ClickHouse | Latest | Data warehouse untuk analisis |
| Metabase | Latest | Dashboard dan visualisasi |
| Docker | - | Containerization semua service |
| Python | 3.11 | Bahasa pemrograman pipeline |
| Pandas | 2.2.1 | Manipulasi dan transformasi data |
| XGBoost | 2.0.3 | Model prediksi keterlambatan |
| SHAP | - | Explainability model ML |
| Great Expectations | 0.18.19 | Data quality validation |
| clickhouse-connect | 0.7.16 | Koneksi Python ke ClickHouse |
| google-api-python-client | 2.126.0 | Akses Google Drive API |
| PyArrow | 15.0.2 | Membaca dan menulis file Parquet |

---

# **Repository Structure**

---

```
Final_Project_Road_to_Admin_MCI/
├── dags/
│   ├── pipeline.py                 # DAG Airflow utama
│   └── scripts/
│       ├── extract.py              # Task 1: Fetch dari Google Drive ke data lake
│       ├── transform.py            # Task 2: Transform + XGBoost ML prediction
│       ├── validate.py             # Task 3: Validasi 25+ checks sebelum load
│       ├── ge_validate.py          # Task 4: Great Expectations HTML report
│       └── load.py                 # Task 5: Load ke ClickHouse
├── sql/
│   └── ddl_operational.sql         # Schema 10 tabel ClickHouse
├── data/
│   └── ge_reports/                 # HTML data quality report (per minggu)
├── credentials/                    # Service account Google Cloud (tidak di-push)
├── Part1_EDA.ipynb                 # Notebook EDA + Statistical Testing + ML + SHAP
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .gitignore
```

---

# **Dataset**

---

Dataset berasal dari [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) yang disimpan di Google Drive dan diakses pipeline secara otomatis via Google Drive API.

**Google Drive Folder ID:** `1JD5Nqljh39cq1Nem5hFsDm6VBSDZ8cpv`

| Dataset | Dipakai | Kolom yang Dipakai | Alasan |
|---|---|---|---|
| orders.csv | Ya | order_id, order_status, semua timestamp | Inti analisis untuk sumber semua perhitungan keterlambatan |
| order_items.csv | Ya | order_id, seller_id, product_id, price, freight_value | Data seller, harga, dan ongkir per order |
| sellers.csv | Ya | seller_id, seller_state, seller_city | Identitas dan lokasi seller |
| customers.csv | Ya | customer_id, customer_unique_id, customer_state, customer_city | Identitas dan lokasi customer |
| order_reviews.csv | Ya | order_id, review_score | Rating customer untuk analisis dampak keterlambatan |
| products.csv | Ya | product_id, product_category_name | Kategori produk untuk analisis per kategori |
| category_translation.csv | Ya | product_category_name, product_category_name_english | Terjemahan kategori dari Portugis ke Inggris |
| order_payments.csv | Tidak | - | Lebih relevan untuk Finance Analyst |
| geolocation.csv | Tidak | - | Metabase free tidak support peta Brazil |
| closed_deals.csv | Tidak | - | Data akuisisi seller, bukan performa pengiriman |
| mql.csv | Tidak | - | Data rekrutmen seller, bukan performa pengiriman |

---

# **Task 1 : Extract Data dari Google Drive**

---

`extract.py` mengambil 7 file CSV dari Google Drive via API, menyimpannya ke data lake dengan timestamp, lalu menyimpan versi terbaru ke folder raw untuk diproses transform.

### 1. Koneksi ke Google Drive API

mendefinisikan path credentials dan konfigurasi folder Google Drive yang akan diakses pipeline.

<img width="1372" height="1432" alt="image" src="https://github.com/user-attachments/assets/51513180-3362-4bc3-aafe-e77621d10d3c" />
<img width="1510" height="862" alt="image" src="https://github.com/user-attachments/assets/cdfd115e-5fd4-4096-b303-004d74d31c44" />

Menggunakan service account credentials untuk autentikasi ke Google Drive API. Credentials disimpan di `credentials/service_account.json` yang tidak di-push ke GitHub karena berisi kunci akses.

### 2. Download dan Simpan ke Data Lake

membuat koneksi ke Google Drive menggunakan service account credentials yang sudah didefinisikan.

<img width="1142" height="634" alt="image" src="https://github.com/user-attachments/assets/ee13d705-0975-4277-891a-2aaf807ed52e" />
<img width="1388" height="1318" alt="image" src="https://github.com/user-attachments/assets/aa7d4839-d9b7-4dd0-b8e8-ea8af45e6437" />

Setiap file CSV di-download dari Google Drive dan disimpan ke dua lokasi:

| Lokasi | Format | Fungsi |
|---|---|---|
| `data/lake/` | `nama_YYYYMMDD_HHMMSS.parquet` | Data lake dengan timestamp untuk versioning |
| `data/raw/` | `nama.parquet` | Raw data terbaru untuk diproses transform |

Data lake menyimpan semua history extract sehingga setiap pipeline jalan akan ada file baru dengan timestamp. Ini untuk replay data kalau ada error di downstream.

---

# **Task 2 : Transform Data**

---

`transform.py` memproses 7 file parquet dari raw, menghitung derived columns, memfilter outlier, melatih model XGBoost, dan menghasilkan 10 tabel output.

### 1. Derived Columns

mem-parsing semua kolom timestamp menjadi format datetime dan menyaring hanya order dengan status delivered.

<img width="2510" height="1166" alt="image" src="https://github.com/user-attachments/assets/6bf2126f-6bab-4d0d-ba72-99f931885225" />

menghitung semua derived columns untuk waktu per tahap (t1/t2/t3), status keterlambatan, waktu pembelian, seller cohort, dan kategori ongkir.

<img width="2218" height="672" alt="image" src="https://github.com/user-attachments/assets/eeeefc8a-79b6-44cd-ad73-eda39fe65bb9" />

Kolom tambahan yang dihitung dari timestamp pengiriman:

| Kolom | Deskripsi |
|---|---|
| t1_approval_h | Waktu approval platform jadi dari order dibuat sampai di-approve (jam) |
| t2_processing_h | Waktu processing seller jadi dari approval sampai diserahkan ke kurir (jam) |
| t3_shipping_h | Waktu pengiriman kurir jadi dari kurir terima sampai customer terima (jam) |
| delivery_days | Total hari pengiriman dari order dibuat sampai diterima customer |
| estimated_days | Total hari estimasi pengiriman yang diberikan platform |
| is_late | 1 jika terlambat dari estimasi, 0 jika tepat waktu |
| days_late | Berapa hari keterlambatannya |
| actual_vs_est_ratio | Rasio aktual vs estimasi dan dipakai untuk hitung SLA accuracy |
| seller_cohort | Kuartal pertama seller punya order (misal 2017Q3) |
| freight_bucket | Kategori ongkir berdasarkan quintile (Sangat murah sampai Sangat mahal) |

### 2. Filter Outlier

memfilter 65 order yang dianggap error sistem, ada 2 order dengan approval delay ekstrem dan 63 order dengan shipping time tidak wajar.

<img width="1542" height="672" alt="image" src="https://github.com/user-attachments/assets/753c76bc-74b6-4f46-b853-8bf9bd7f05e1" />

2 jenis outlier dikeluarkan berdasarkan investigasi mendalam:

**System Error Approval (2 order)**

2 order dengan approval delay 741 jam ditemukan berasal dari 2 seller berbeda yang di-approve selisih 14 menit pada Januari–Februari 2018. Terbukti system error platform, bukan realitas bisnis.

**Batch Update Shipping (63 order)**

63 order dengan shipping time lebih dari 3000 jam ditemukan tiba pada tanggal yang sama (19 September 2017) dari 39 seller berbeda. Terbukti batch update sistem, bukan pengiriman aktual.

Total 65 order dikeluarkan sehingga sisa 96.413 dari 96.478 order delivered.

### 3. ML Model — Late Delivery Prediction

menyiapkan features dan melakukan encoding kolom kategorikal sebelum training model.

<img width="2450" height="634" alt="image" src="https://github.com/user-attachments/assets/759d567d-6399-47b2-a515-120495f44fa9" />

melatih model XGBoost dengan parameter scale_pos_weight untuk menangani data yang tidak seimbang antara order tepat waktu dan terlambat.

<img width="1434" height="824" alt="image" src="https://github.com/user-attachments/assets/839c5e7a-b56c-455a-b3e8-81f4e7fd9967" />

untuk prediksi probabilitas keterlambatan per order dan mengkategorikannya ke dalam tiga tingkat risiko.

<img width="1326" height="710" alt="image" src="https://github.com/user-attachments/assets/63d46b3d-81a7-4f8b-9eee-a9aba90c1599" />

Model XGBoost dilatih di dalam transform untuk memprediksi apakah sebuah order akan terlambat. Hasil prediksi disimpan sebagai tabel `fact_late_risk` di ClickHouse.

| Metrik | Random Forest | XGBoost |
|---|----|---|
| ROC-AUC | 0.7236 | 0.7378 |
| Recall (late) | 51% | 60% |
| Precision (late) | 20% | 17% |

XGBoost dipilih karena ROC-AUC lebih tinggi dan recall lebih baik — lebih banyak order terlambat yang berhasil dideteksi lebih awal.

**Features:** customer_state, seller_state, purchase_month, purchase_dayofweek, purchase_hour, freight_bucket, seller_cohort

**Output per order:** late_probability, late_prediction, risk_level (Risiko Rendah / Risiko Sedang / Risiko Tinggi)

### 4. Simpan Hasil Transform

menyimpan semua 10 tabel hasil transform ke format parquet untuk kemudian di-load ke ClickHouse.

<img width="1572" height="1090" alt="image" src="https://github.com/user-attachments/assets/c403e03c-164e-4ea6-9867-89de729c7f64" />

---

# **Task 3 : Validasi Data**

---

`validate.py` bertugas memastikan data hasil transform memenuhi standar kualitas sebelum masuk ke ClickHouse. Pipeline otomatis berhenti kalau ada validasi yang gagal.

### Daftar Validasi

| Validasi | Ekspektasi |
|---|---|
| Row count fact_deliveries | Lebih dari 90.000 rows |
| Outlier sudah difilter | t1 maksimal 700 jam, t3 maksimal 3000 jam |
| Kolom kritis tidak null | order_id, is_late, customer_state, purchase_month, seller_cohort, freight_bucket |
| Tidak ada nilai negatif | t1, t2, t3 semua non-negatif |
| is_late hanya 0 atau 1 | Tidak ada nilai lain |
| Late rate masuk akal | Antara 6-12% |
| purchase_month valid | Antara 1-12 |
| is_chronic logika benar | Hanya seller dengan total_orders >= 20 |
| Semua agg tables tidak kosong | agg_seller, agg_regional, agg_monthly, agg_cohort, kpi_summary |
| fact_late_risk valid | risk_level hanya 3 kategori, late_probability antara 0-1 |

Hasil validasi run terakhir: **25 PASS, 0 FAIL**

### 1. Setup & Load Data

memuat semua file parquet hasil transform yang akan divalidasi sebelum masuk ke ClickHouse.

<img width="1542" height="1356" alt="image" src="https://github.com/user-attachments/assets/33056c95-1afc-43f4-a708-41eb7a32ceb5" />

### 2. Validasi Data

Bagian ini menjalankan 25+ pengecekan otomatis dari row count, outlier, kolom null, nilai negatif, logika is_chronic, sampai validasi fact_late_risk.

<img width="2326" height="2572" alt="image" src="https://github.com/user-attachments/assets/a745ff8c-accf-4935-8a18-f80cbccc905a" />

### 3. Summary & Pipeline Stop

Bagian ini mencetak hasil validasi dan menghentikan pipeline otomatis kalau ada yang gagal.

<img width="1528" height="634" alt="image" src="https://github.com/user-attachments/assets/2359fbf9-29b8-4e56-a13b-9e635eb6d6a9" />

---

# **Task 4 : Great Expectations Data Quality Report**

---

`ge_validate.py` menjalankan 9 validasi menggunakan Great Expectations dan menghasilkan HTML report yang disimpan di `data/ge_reports/`.

### Cara Kerja Report

Report disimpan per minggu dengan format `data_quality_report_YYYY-WNN.html`. Setiap pipeline jalan dalam minggu yang sama, run baru di-append ke file yang sama di bagian Riwayat Run Minggu Ini sehingga dalam satu file bisa ada beberapa run harian.

File lama otomatis dihapus kalau sudah lebih dari 4 minggu, jadi maksimal selalu ada 4 file di folder.

### Daftar Validasi GE

| Check | Kolom |
|---|---|
| order_id tidak null | order_id |
| is_late hanya 0 atau 1 | is_late |
| Late rate 6-12% | is_late |
| purchase_month antara 1-12 | purchase_month |
| Tidak ada approval delay ekstrem | t1_approval_h |
| Tidak ada shipping ekstrem | t3_shipping_h |
| Row count 90K-100K | table |
| seller_cohort tidak null | seller_cohort |
| freight_bucket valid | freight_bucket |

Hasil run terakhir: **9/9 PASS**

### 1. Setup & Auto-delete Report Lama

mendefinisikan path output dan menghapus otomatis report yang sudah lebih dari 4 minggu.

<img width="1464" height="824" alt="image" src="https://github.com/user-attachments/assets/cff6a037-b601-4239-8df4-385fcb1c60a8" />

### 2. Great Expectations Validation

Bagian ini menjalankan 9 validasi menggunakan Great Expectations terhadap fact_deliveries.

<img width="1588" height="1546" alt="image" src="https://github.com/user-attachments/assets/60364b0d-8b48-4b2f-92d4-cef93cf529fe" />

### 3. Generate HTML Report & Pipeline Stop

Bagian ini membuat HTML report mingguan dengan append run harian dan menghentikan pipeline kalau ada validasi yang gagal.

<img width="4342" height="3142" alt="image" src="https://github.com/user-attachments/assets/26a2dab3-9ce2-47a3-83ba-adb0ad1ba93c" />

### 4. Hasil Report

<img width="1918" height="990" alt="image" src="https://github.com/user-attachments/assets/f0bb0efb-3e0f-4c13-980e-e3d13eb47f8e" />

---

# **Task 5 : Load ke ClickHouse**

---

`load.py` bertugas men-drop semua tabel lama, membuat ulang dari DDL, lalu insert semua data hasil transform ke ClickHouse.

### Database Schema

`dustinia_db` terdiri dari 10 tabel:

| Tabel | Deskripsi | Rows |
|---|---|---|
| fact_deliveries | Tabel utama denormalized — semua order dengan derived columns | 96.942 |
| dim_sellers | Dimensi seller | 3.095 |
| dim_customers | Dimensi customer | 99.441 |
| dim_products | Dimensi produk | 32.951 |
| agg_seller_performance | Performa seller aktif (>=20 orders) dengan flag is_chronic dan is_new_problem | 793 |
| agg_regional_stats | Statistik keterlambatan per state | 27 |
| agg_monthly_trend | Tren late rate per bulan dalam format year_month | 23 |
| agg_cohort_performance | Performa per cohort seller (per kuartal) | 9 |
| kpi_summary | 5 KPI pre-computed dengan actual_value, target_value, dan status | 5 |
| fact_late_risk | Hasil prediksi XGBoost per order (late_probability dan risk_level) | 96.478 |

### 1. Koneksi ke ClickHouse

membuat koneksi ke ClickHouse menggunakan clickhouse-connect dengan kredensial yang dikonfigurasi.

<img width="1080" height="634" alt="image" src="https://github.com/user-attachments/assets/6916594f-4ede-43b9-bba6-2e494d04ba73" />

### 2. Buat Database & Tabel dari DDL

membuat database dustinia_db, menghapus semua tabel lama, lalu membuat ulang dari DDL.

<img width="1418" height="1166" alt="image" src="https://github.com/user-attachments/assets/e3376083-49dc-49df-96c6-b489d1edb2c9" />

### 3. Insert Data ke Semua Tabel

membaca semua parquet hasil transform dan memasukkannya ke ClickHouse satu per satu.

<img width="1386" height="1166" alt="image" src="https://github.com/user-attachments/assets/81d28fc5-a31a-4ec3-a66e-21acea011134" />

### 4. Hasil di ClickHouse

<img width="1918" height="1027" alt="image" src="https://github.com/user-attachments/assets/ba44ef95-137f-498d-a065-3bfe5058fe29" />

---

# **Airflow DAG**

---

DAG `dustinia_operational_pipeline` berjalan `@weekly` dengan 5 task berurutan:

```
start → extract → transform → validate → load_to_clickhouse → ge_validate → end
```

| Task | Fungsi |
|---|---|
| extract | Download 7 CSV dari Google Drive, simpan ke data lake dan raw |
| transform | Proses data, hitung derived columns, filter outlier, train XGBoost |
| validate | 25+ validasi — pipeline berhenti kalau ada yang gagal |
| load_to_clickhouse | Drop dan recreate semua tabel, insert ke ClickHouse |
| ge_validate | Great Expectations validation, generate HTML report mingguan |

alur task pipeline di Airflow:

<img width="1516" height="695" alt="image" src="https://github.com/user-attachments/assets/15783f55-7ff5-4e7b-b927-5b5e405a3594" />

Riwayat run pipeline:

<img width="757" height="386" alt="image" src="https://github.com/user-attachments/assets/0f949fe6-b937-4d7e-9bf6-58962c9a0bab" />

---

# **KPI Operasional**

---

| KPI | Aktual | Target | Status |
|---|---|---|---|
| On-Time Delivery Rate | 91.9% | >95% | ❌ |
| Customer Tolerance Breach Rate | 4.8% | <2% | ❌ |
| SLA Accuracy Rate | 0.53 | 0.8-1.0 | ❌ |
| Late Rate per Seller (avg aktif) | 7.7% | <5% | ❌ |
| Processing Time Compliance | 53.2% | >80% | ❌ |

Semua KPI ini saya tetapkan berdasarkan kondisi aktual platform dan standar yang masuk akal untuk dikejar. On-Time Delivery Rate ditarget 95% karena itu angka yang umum dijadikan benchmark di industri logistik. Customer Tolerance Breach Rate ditarget di bawah 2% karena dari data terbukti keterlambatan lebih dari 3 hari langsung bikin review score drastis. SLA Accuracy Rate ditarget 0.8-1.0 supaya estimasi yang diberikan ke customer lebih jujur dan tidak terlalu jauh dari kenyataan. Late Rate per Seller ditarget di bawah 5% karena angka itu sekitar 2.5x rata-rata platform jadi seller yang di atas itu sudah bisa dibilang bermasalah secara konsisten. Processing Time Compliance ditarget 80% karena seller yang proses lebih dari 48 jam terbukti jadi salah satu penyebab utama keterlambatan.

Semua KPI belum tercapai sehingga KPI ini sebagai dasar seluruh analisis dari Final Project ini.

---

# **Temuan Utama EDA**

---

### 1. Bottleneck di Kurir

t3 shipping untuk order terlambat rata-rata 600 jam vs 190 jam untuk yang tepat waktu, 3x lebih lama. Ini berarti bottleneck utama ada di tahap pengiriman kurir, bukan di approval platform atau processing seller.

### 2. SLA Sandbagging Sistematis

61.2% order tiba dalam kurang dari 50% estimasi yang diberikan. SLA accuracy rate hanya 0.53 karena platform memberikan estimasi yang terlalu longgar ke customer di semua wilayah.

### 3. Seasonal Pattern

Maret late rate 17.2% dan November 14.3%, memuncak setiap tahun. Black Friday tidak menyebabkan spike di hari H (24 November) tapi efek domino di 25-27 November karena sistem kewalahan memproses lonjakan order.

### 4. 29 Seller Kronis

29 seller aktif dengan late rate lebih dari 20% dan minimal 20 orders. Seller yang bergabung di 2017Q3-2018Q1 paling bermasalah dengan late rate 8.5-9.8%.

### 5. Gap Regional Ekstrem

Alagoas late rate 23.9% vs São Paulo 5.9%, 4x lipat. Dilihat dari cross-analysis bahwa seller normal pun late rate-nya 23.7% ke Alagoas, sehingga masalahnya bukan hanya dari seller kronis tapi juga infrastruktur logistik ke Nordeste.

### 6. Customer Tolerance Threshold

Telat lebih dari 3 hari review score 3.29, telat lebih dari 4 hari drastis ke 2.10. Review score tepat waktu 4.29 vs terlambat 2.56.

### 7. Rekomendasi Estimasi Pengiriman

Estimasi terlalu longgar di semua wilayah. Rekomendasi percentile 75 data aktual dan jika diterapkan SLA accuracy rate bisa jadi naik dari 0.53 mendekati 0.75.

---

# **Metabase Dashboard**

---

Dashboard terdiri dari 6 seksi yang bercerita dari kondisi saat ini sampai prediksi ke depan:

| Seksi | Konten |
|---|---|
| Kondisi Operasional Saat Ini | KPI Summary + Total Order Terlambat + Revenue At Risk + Seller Kronis |
| Di Mana Masalahnya? | Bottleneck per tahap pengiriman + Tren late rate bulanan |
| Siapa yang Bermasalah? | Daftar seller kronis + Distribusi kategori seller + Cohort seller |
| Wilayah Mana yang Bermasalah? | Keterlambatan per wilayah + Cross analysis seller vs wilayah + Rekomendasi estimasi |
| Dampak ke Pelanggan | Review score + Kategori produk revenue at risk + Keterlambatan per ongkir |
| Prediksi & Early Warning | Distribusi risiko keterlambatan dari model XGBoost |

<img width="1215" height="810" alt="image" src="https://github.com/user-attachments/assets/3906698c-ae19-4e75-b01c-2deae90be150" />
<img width="1095" height="871" alt="image" src="https://github.com/user-attachments/assets/efac1d55-14e0-44eb-8467-51f178df68e1" />
<img width="1008" height="952" alt="image" src="https://github.com/user-attachments/assets/58995696-1ccd-447c-87e4-3cbb3b541b39" />
<img width="1007" height="902" alt="image" src="https://github.com/user-attachments/assets/91178aec-ca31-4f78-b640-1ca07757f81b" />
<img width="1028" height="778" alt="image" src="https://github.com/user-attachments/assets/00795f79-683a-4742-b9cb-2ff18c593021" />
<img width="1015" height="396" alt="image" src="https://github.com/user-attachments/assets/17f1c95e-87a0-4a9f-8399-ab5065726b4a" />
<img width="1010" height="946" alt="image" src="https://github.com/user-attachments/assets/dc93a006-737b-4752-beab-56be8c41db6a" />
<img width="1017" height="477" alt="image" src="https://github.com/user-attachments/assets/90ec23d2-27d5-41e7-a0ad-efbab181aebe" />

---

# **Akses Service**

---

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Metabase | http://localhost:3000 | setup saat pertama buka |
| ClickHouse | http://localhost:8123 | admin / dustinia2026 |

---

# **Notebook EDA**

---

`Part1_EDA.ipynb` berisi analisis lengkap dengan urutan:

1. Import & Load Data
2. Data Overview
3. Preprocessing
4. Univariate Analysis
5. Late Delivery Analysis — bottleneck, seasonal, SLA sandbagging, anomali
6. Seller Performance Analysis
7. Regional Analysis
8. Product Category Analysis
9. Cross Analysis Operasional × Customer Experience
10. Correlation Heatmap
11. Outlier Analysis + Investigasi + Keputusan
12. Cohort Analysis Seller
13. Freight Value Analysis
14. Peak Time & Special Event Analysis — Black Friday, Workday vs Weekend, Time of Day
15. Statistical Significance Testing — 5 uji chi-square
16. EDA Summary — 12 temuan + rekomendasi bisnis
17. KPI
18. Business Impact Quantification
19. Late Delivery Prediction (ML) — Random Forest vs XGBoost
20. Model Explainability (SHAP)

---

# **Kesimpulan**

---

Keterlambatan pengiriman di DustiniaDelixia Groceria disebabkan dua hal utama yaitu:
1. kurir yang butuh waktu 3x lebih lama untuk order yang terlambat
2. 29 seller yang secara konsisten bermasalah.
Dampaknya sangat terasa karena ada 7.823 order terlambat dengan review score customer drastis dari 4.29 ke 2.56.

Selain itu platform juga memberikan estimasi pengiriman yang terlalu jauh ke customer di semua wilayah. Ini bukan penyebab keterlambatan tapi berdampak ke daya saing platform. Customer yang mau membeli produk yang sama akan cenderung pilih platform dengan estimasi tiba paling cepat, meskipun pada kenyataannya platform dengan estimasi lebih lama bisa datang duluan. Estimasi yang tidak akurat membuat platform kalah bersaing di mata customer bahkan sebelum barang dikirim. Sehingga rekomendasi estimasi per wilayah bisa dipakai untuk meningkatkan SLA accuracy rate dari 0.53 mendekati 0.75 sekaligus membuat platform lebih kompetitif.

Model XGBoost yang dibangun bisa menebak 60% order yang bisa jadi terlambat sebelum keterlambatan benar-benar terjadi. sehingga tim operasional dapat mengambil tindakan lebih awal tanpa harus menunggu customer komplain.
