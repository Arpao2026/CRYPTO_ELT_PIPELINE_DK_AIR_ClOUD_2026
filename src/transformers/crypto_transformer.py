import sqlite3
import json
import os
from typing import List, Dict, Any, Tuple
from config.settings import settings
from src.utils.logger import logger

class CryptoTransformer:
    """
    Handles the Transformation and Core Loading phase (T & L in ETL).
    Implements business logic, data filtering, and auditing of rejected records.
    """
    
    def __init__(self):
        # Initialize database path from centralized settings
        self.db_path = settings.DATABASE_URL.replace('sqlite:///', '').strip()
    
    def get_cleaned_data(self, batch_id: str = None) -> List[Tuple]:
        """
        Extracts raw data from Staging and coordinates the transformation process.
        
        Args:
            batch_id (str, optional): Filters extraction to a specific run. 
            If None, fetches all available records.
        """
        try:
            logger.info(f"Extracting raw data for processing (Batch: {batch_id})")

            # Incremental Loading Logic: Process only specific batch if ID is provided
            if batch_id:
                select_query = 'SELECT raw_data FROM stg_crypto_markets WHERE batch_id = ?'
                params = (batch_id,)
            else:
                select_query = 'SELECT raw_data FROM stg_crypto_markets'
                params = ()
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(select_query, params)
                rows = cursor.fetchall()

            if not rows:
                logger.warning(f"No records found in staging for batch: {batch_id}")
                return []
            
            # Delegate raw data to internal transformation logic
            return self.transform_logic(rows, batch_id)

        except Exception as e:
            logger.error(f"Failed to extract data from Staging: {str(e)}")
            return []
    
    def transform_logic(self, rows: List[Tuple], batch_id: str = None) -> List[Tuple]:
        """ 
        Parses JSON payloads and applies business transformation rules.
        Implements Data Quality filtering to remove inactive or incomplete assets.
        """
        cleaned_data = []
        data_issues = [] 
        
        for row in rows:
            try:
                # Deserialize JSON string back to Python Dictionary
                item = json.loads(row[0])
                price = item.get('current_price', 0)
                volume = item.get('total_volume', 0)

                # Map raw data to the Core Fact Table schema
                record = {
                    'batch_id': batch_id,
                    'id': item.get('id'),
                    'symbol': item.get('symbol'),
                    'name': item.get('name'),
                    'price': price,
                    'market_cap': item.get('market_cap', 0),
                    'total_volume': volume,
                    'last_updated': item.get('last_updated')
                }

                # Transformation Rule: Keep only active assets (Price & Volume > 0)
                if price > 0 and volume > 0:
                    cleaned_data.append(tuple(record.values()))
                else:
                    # Capture rejected records for Data Quality auditing
                    record['reason'] = 'Invalid asset: Zero price or volume detected'
                    data_issues.append(record)
                    
            except (json.JSONDecodeError, TypeError) as e:
                logger.error(f"Data corruption detected - JSON Parsing Error: {e}")
                continue

        # Export quality issues for external observability
        if data_issues:
            self.save_issues_to_json(data_issues, batch_id)

        logger.info(f"Transformation complete: {len(cleaned_data)} valid, {len(data_issues)} rejected.")
        return cleaned_data 
        
    def save_issues_to_json(self, issues: List[Dict], batch_id: str = None):
        """ 
        Persists rejected records to JSON files for Root Cause Analysis (RCA).
        Follows a batch-partitioned storage pattern.
        """
        try:
            folder_path = 'data/issues'
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)

            file_name = f'issues_{batch_id}.json' if batch_id else 'issues_unknown.json'
            file_path = os.path.join(folder_path, file_name)

            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(issues, f, ensure_ascii=False, indent=4)

            logger.info(f"Data Quality Report generated: {file_path}")
        except Exception as e:
            logger.error(f"Auditing failure: Could not save issue report: {e}")
            
    def save_to_core(self, data: List[Tuple]):
        """
        Loads transformed data into the Core Fact Table using an Append-only pattern.
        Maintains historical snapshots using a Composite Primary Key.
        """
        if not data:
            logger.warning("Ingestion skipped: No valid records to load.")
            return
        
        try:
            # DDL: Define the History Fact Table (Time-series data model)
            # Composite Key (batch_id, coin_id) ensures data integrity and prevents duplicates
            create_table_query = '''
            CREATE TABLE IF NOT EXISTS fct_crypto_prices(
                batch_id TEXT,
                coin_id TEXT,
                symbol TEXT,
                name TEXT,
                price REAL,
                market_cap REAL,
                total_volume REAL,
                last_updated_at TIMESTAMP,
                PRIMARY KEY (batch_id, coin_id)
            )
            '''
            
            # DML: Execute bulk insert using 'OR IGNORE' for Idempotency
            insert_query = '''
            INSERT OR IGNORE INTO fct_crypto_prices
            (batch_id, coin_id, symbol, name, price, market_cap, total_volume, last_updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            '''

            with sqlite3.connect(self.db_path) as conn:
                conn.execute(create_table_query)
                # Bulk insertion for optimal throughput
                conn.executemany(insert_query, data)
                conn.commit()
                logger.info(f"DATABASE LOAD SUCCESS: {len(data)} records archived in Fact Table.")
        
        except Exception as e:
            logger.error(f"Core Layer Load Error: {str(e)}")
            raise