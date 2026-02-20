from src.extractors.coingecko import CoingeckoClient
from src.loaders.sqlite_loader import SQLiteLoader
from src.transformers.crypto_transformer import CryptoTransformer
from src.quality.data_quality import DataqualityValidator
from src.utils.logger import logger

def run_pipeline():
    """
    Orchestrates the End-to-End ETL Pipeline process.
    Flow: Extract (API) -> Load (Staging) -> Transform (Logic) -> DQ (Audit) -> Load (Core)
    """
    logger.info('--- Initiating Cryptocurrency ELT Pipeline ---')

    # 1. Extraction Phase (API Interaction)
    # Instantiate the client which includes built-in retry logic (Tenacity)
    client = CoingeckoClient()
    raw_data = client.get_coin_market()

    if not raw_data:
        logger.error('Pipeline Aborted: No data retrieved from API.')
        return

    # 2. Loading Phase (Staging / Data Lake)
    # Saves immutable raw JSON data for traceability and historical audit.
    loader = SQLiteLoader()
    current_batch_id = loader.load_to_staging(raw_data)

    # 3. Transformation Phase (Processing & Filtering)
    # Extracts from Staging and applies business rules (e.g., filtering inactive assets)
    transformer = CryptoTransformer()
    cleaned_data = transformer.get_cleaned_data(batch_id=current_batch_id)

    # 4. Data Quality Validation (Gatekeeper)
    # Ensures data integrity (e.g., price > 0) before final ingestion.
    dq = DataqualityValidator()
    
    if dq.validate_market_data(cleaned_data):
        # 5. Final Load Phase (Data Warehouse / Core Fact Table)
        # If DQ passes, persist the refined records into the production table.
        transformer.save_to_core(cleaned_data)
        logger.info('--- Pipeline Execution Completed Successfully ---')
    else:
        # Critical Alert: Data integrity issues found; prevent corrupted data from entering Core.
        logger.error('Pipeline Halted: Data Quality validation failed. Ingestion cancelled.')

# Entry point of the script
if __name__ == '__main__':
    run_pipeline()