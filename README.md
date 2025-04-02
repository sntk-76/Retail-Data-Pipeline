# 🛠️ Retail Data Pipeline Project

This project implements an end-to-end data pipeline using modern cloud and data engineering tools. It automates the flow of retail sales data from raw CSV upload to transformation, storage in BigQuery, and interactive visualization via Looker Studio.

📊 **[View Dashboard in Looker Studio](https://lookerstudio.google.com/reporting/32142238-71f8-4c7c-8dc2-45038440d426)**

---

## 🚀 Pipeline Overview

```mermaid
graph LR
    A[Terraform Setup] --> B[Google Cloud Infrastructure (GCS, BigQuery)]
    B --> C[Raw Data Upload (Airflow)]
    C --> D[Raw Data in GCS Bucket]
    D --> E[Raw Data Load to BigQuery (Airflow)]
    D --> F[Jupyter Notebook]
    F --> G[PySpark Data Transformation]
    G --> H[Cleaned CSV Export]
    H --> I[Clean Data Upload to GCS (Airflow)]
    I --> J[Clean Data to BigQuery (Airflow)]
    J --> K[Data Visualization in Looker Studio]
```

---

## 📂 Project Structure

```
Retail-Data-Pipeline/
├── Infrastructure/           # Terraform IaC for GCS + BigQuery
├── Airflow/                  # DAGs, configs, and logs
│   ├── dags/                 # DAGs for raw and clean data
│   ├── data/                 # Raw and clean CSVs
│   ├── keys/                 # GCP service account keys
├── Notebooks/                # Jupyter notebooks for transformation
├── Dashboard/                # Looker Studio PDF Export
├── Scripts/                  # Optional scripts
├── requirements.txt          # Python dependencies
├── Makefile                  # Automation (optional)
└── README.md
```

---

## 🧱 Technologies Used

| Tool/Service     | Role                                |
|------------------|--------------------------------------|
| Terraform        | Infrastructure provisioning          |
| Google Cloud     | Cloud provider (GCS, BigQuery)       |
| Apache Airflow   | Workflow orchestration               |
| Apache Spark     | Distributed data processing          |
| Jupyter Notebook | Local transformation in PySpark      |
| Looker Studio    | Interactive data visualization       |
| Docker           | Local development & Airflow setup    |

---

## 📌 Pipeline Phases

### Phase 1: Infrastructure Setup
- Provisioned a GCP Bucket and BigQuery dataset using Terraform.

### Phase 2: Raw Data Ingestion
- Created Airflow DAG to upload CSV to GCS.
- Created another DAG to load it from GCS to BigQuery.

### Phase 3: Data Transformation
- Performed transformation in Jupyter using PySpark.
- Cleaned, casted, deduplicated, and standardized the data.
- Exported the transformed data as CSV.

### Phase 4: Load Transformed Data
- Used Airflow DAG to upload cleaned data to GCS.
- Loaded it to a new BigQuery table using another DAG.

### Phase 5: Visualization
- Built interactive dashboard in Looker Studio with charts and filters.
- Link: [Retail Looker Dashboard](https://lookerstudio.google.com/reporting/32142238-71f8-4c7c-8dc2-45038440d426)

---

## 📸 Pipeline Architecture Diagram

![Pipeline Diagram](Dashboard/report/Retail_data_pipeline.pdf)

---

## 📄 License

This project is licensed under the MIT License.

---

## ✨ Credits

Created with ❤️ by Sina Tavakoli

