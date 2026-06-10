---

# **DustiniaDelixia Groceria — Operational Analytics Pipeline**

---

> Seleksi Admin Lab MCI 2026 | Final Project — Operational Analyst

| Nama | NRP |
|------|-----|
| Rahma Sakinah | 5025241222 |

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
