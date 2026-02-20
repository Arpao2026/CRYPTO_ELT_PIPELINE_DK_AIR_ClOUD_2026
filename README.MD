# 🚀 Crypto ELT Pipeline

## 📝 Project Overview
An automated **End-to-End Data Pipeline** designed to ingest cryptocurrency market data. This project follows the **ELT (Extract-Load-Transform)** architecture, leveraging a **Data Lake** concept for raw data persistence before applying business logic and loading refined datasets into a structured **Data Warehouse** (SQLite).

---

## 🏗️ System Architecture

### 1. Extraction (API Layer)
* **Source:** CoinGecko API.
* **Resilience:** Implemented **Exponential Backoff** retry mechanism using `tenacity`.
* **Authentication:** Secure header management for API key integration.
* **Error Handling:** Robust `try-except` blocks to ensure graceful failure and logging.

### 2. Staging (Data Lake Concept)
* **Storage:** Raw JSON data is persisted in the `stg_crypto_markets` table.
* **Schema-on-Read:** Stores full JSON payloads to ensure data lineage and the ability to re-process historical data without re-fetching from the source.
* **Traceability:** Every record is tagged with a `batch_id` and `extracted_at` timestamp.

### 3. Transformation (Business Logic)
* **Incremental Processing:** Fetches data based on `batch_id` for efficient processing.
* **Filtering Logic:** Only "Active Assets" are promoted to the final layer.
    * **Rule:** `Price > 0` AND `Volume > 0`
    * **Objective:** To filter out 'Zombie Coins' or assets with zero liquidity, ensuring high-quality analytical data.
* **Audit Trail:** Rejected records are exported to a `data/issue` JSON directory for further **Root Cause Analysis (RCA)**.

### 4. Data Quality (DQ) Validation
* **Integrity Checks:** Acts as a **circuit breaker** before data ingestion.
* **Validation Rules:** Ensure all transformed records are list of tuples with valid data types and non-negative financial values.

### 5. Loading (Data Warehouse)
* **Destination:** Structured `fct_crypto_prices` table.
* **Strategy:** **Append-only** history tracking using a **Composite Primary Key** (`batch_id`, `coin_id`) to prevent duplicate ingestion while maintaining a time-series record.

---

## 🛠️ Tech Stack
* **Language:** Python 3.13
* **Database:** SQLite (Relational Database)
* **Core Libraries:** `requests`, `tenacity` (Resiliency), `python-dotenv` (Config Management)
* **Testing:** `pytest`, `pytest-mock` (Unit Testing & Mocking)
* **Logging:** Centralized Python logging for monitoring pipeline health.

---

## 🧪 Quality Assurance & Testing
1. **Unit Testing:** Comprehensive tests for transformation logic and data validation rules.
2. **API Mocking:** Utilized `pytest-mock` to simulate various API scenarios:
    * **Success Case:** Verified correct parsing of standard API responses.
    * **Failure Case:** Simulated 500 Server Error and Network Timeouts to verify that the **Retry Mechanism** and **Exception Handling** work as expected without crashing the pipeline.

---

## 🚀 How to Run
1. **Clone the repository:**
   ```bash
   git clone https://github.com/Arpao2026/crypto-etl-pipeline-v2.git

2. **Install dependencis:**
    ```bash
    pip install -r requirements.txt

3. **Configure Environment:** Create a .env file and add your COINGECKO_API_KEY.
4. **Run The Pipeline:**
    ```bash
    python main.py

5. **Run Tests:**
    ```bash
    python -m pytest