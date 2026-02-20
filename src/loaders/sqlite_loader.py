import sqlite3
import json
from typing import List, Dict, Any, Tuple
from datetime import datetime
from config.settings import settings
from src.utils.logger import logger

class SQLiteLoader:
    """
    Handles the 'Load' phase of the ELT process by persisting raw data 
    into a SQLite staging area (Data Lake).
    """
    
    def __init__(self):
        # Extract the database file path by removing the SQLAlchemy prefix
        self.db_path = settings.DATABASE_URL.replace('sqlite:///', '').strip()
        try:
            logger.info(f"Initializing SQLite staging at: {self.db_path}")
            # Ensure the staging schema exists upon instantiation
            self._create_staging_table()
        except Exception as e:
            logger.error(f"Failed to initialize SQLiteLoader: {str(e)}")
            raise

    def _create_staging_table(self):
        """
        Creates a staging table to store immutable raw JSON data.
        Schema includes batch_id for data traceability and auditing.
        """
        query = '''
        CREATE TABLE IF NOT EXISTS stg_crypto_markets(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            coin_id TEXT,               -- Unique identifier for the asset (e.g., 'bitcoin')
            raw_data TEXT,              -- Full JSON payload stored as a string
            extracted_at TIMESTAMP,     -- Timestamp of the extraction event
            batch_id TEXT               -- Unique ID to group records from the same pipeline run
        );
        '''
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(query)
            logger.debug("Staging table 'stg_crypto_markets' is ready.")

    def load_to_staging(self, data: List[Dict[str, Any]]) -> str:
        """
        Persists a list of dictionaries into the staging table using bulk insertion.
        
        Args:
            data (List[Dict[str, Any]]): Raw data records fetched from the API.
            
        Returns:
            str: The generated batch_id for this loading session.
        """
        try:
            # Generate a unique batch_id for incremental loading tracking
            batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')
            extracted_at = datetime.now().isoformat()

            # Using Parameterized SQL to prevent SQL Injection attacks
            insert_query = ''' 
            INSERT INTO stg_crypto_markets (coin_id, raw_data, extracted_at, batch_id)
            VALUES (?, ?, ?, ?)
            '''
            
            # Prepare data for high-performance bulk insertion (List of Tuples)
            rows_to_insert = [
                (item.get('id'), json.dumps(item), extracted_at, batch_id)
                for item in data
            ]

            # Execute bulk insert within a transaction block
            with sqlite3.connect(self.db_path) as conn:
                conn.executemany(insert_query, rows_to_insert)
                conn.commit()
                logger.info(f"Successfully loaded {len(rows_to_insert)} records to Staging (Batch: {batch_id})")

            # Return batch_id to be used by the subsequent Transform phase
            return batch_id

        except Exception as e:
            logger.error(f"Load to staging failed: {str(e)}")
            raise