# 🚀 Crypto Market ELT Pipeline(Learning Project)

## 📝 Project Overview
This project is my hands-on experience learning the ropes of **Data Engineering**. I built an automated end-to-end pipeline that fetches cryptocurrency market data, processes it, and stores it in a structured format for analysis.

---

## 🏗️ How it Works (The Pipeline)

### 1. Data Extraction (Extract)
* **Source:** Real-time data from **CoinGecko API**.
* **Resiliency:** Used Python's `tenacity` library to handle API failures with an automatic **Retry** mechanism. If the API is down, the system waits and tries again instead of crashing.

### 2. Raw Data Storage (Staging)
* **Data Lake Concept:** All raw JSON responses are saved directly into the `stg_crypto_markets` table.
* **Traceability:** I use `batch_id` and `extracted_at` to keep track of when each piece of data was pulled.

### 3. Cleaning & Quality Control (Transform)
* **Business Logic:** I wrote logic to filter out "Zombie Coins." Only assets with **Price > 0** and **Volume > 0** are moved to the final layer.
* **Error Handling:** Any records that fail validation are exported to `data/issue` for later investigation (Root Cause Analysis).

### 4. Automation & Orchestration (Airflow)
* **Scheduling:** Managed by **Apache Airflow**. The pipeline is scheduled to run automatically twice a day at **05:00** and **17:00**.
* **Configuration:** Set up with `catchup=False` to ensure the system stays focused on the most recent data intervals.

---

## 🐳 Infrastructure & Deployment
* **Cloud:** Hosted on **Google Cloud Platform (GCP)** using a Compute Engine instance.
* **Containerization:** The entire stack (Airflow, Postgres, and the ETL app) is containerized using **Docker Compose** for easy deployment and environment consistency.
* **Database:** Uses **SQLite** as the primary Data Warehouse for storing refined price data.

---

## 🛠️ Tech Stack
* **Language:** Python 3.13
* **Orchestration:** Apache Airflow
* **Containerization:** Docker & Docker Compose
* **Cloud:** Google Cloud (GCE)
* **Database:** SQLite & SQL
* **Testing:** `pytest` & `pytest-mock`

---

## 🧪 What I Learned (So Far)
* **Troubleshooting:** Learning how to fix Airflow schedule issues and database locks was a huge part of this journey.
* **SQL Skills:** Gained hands-on experience managing data using SQL commands like `SELECT`, `DELETE`, and `COUNT`.
* **System Monitoring:** Learned to use the Airflow UI to track the health of my "Data Pipes" via phone and computer.

---

## 🚀 How to Run
1. **SSH** into the server.
2. Navigate to the project folder: `cd ~/crypto_elt`
3. Start the system: 
   ```bash
   docker compose up -d