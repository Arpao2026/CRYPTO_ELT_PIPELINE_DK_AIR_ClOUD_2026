from src.utils.logger import logger

class DataqualityValidator:
    """
    Acts as the final 'Gatekeeper' in the pipeline.
    Ensures data integrity and compliance with business rules before persistence.
    """
    def __init__(self):
        pass

    def validate_market_data(self, data: list) -> bool:
        """
        Performs critical data quality checks on the transformed dataset.
        
        Args:
            data (list): A list of tuples containing transformed market records.
            Schema index: (batch_id[0], coin_id[1], symbol[2], name[3], 
            price[4], market_cap[5], volume[6], updated[7])
            
        Returns:
            bool: True if the entire batch meets quality standards, False otherwise.
        """
        if not data:
            logger.warning('DQ Check: No data provided for validation.')
            return False
        
        logger.info(f'Executing Data Quality (DQ) checks for {len(data)} records...')
        
        is_valid = True
        error_count = 0

        for item in data:
            # Mapping indices from the Transformer's output tuple
            # Note: Transformer now includes batch_id at index 0
            coin_id = item[1]
            price = item[4]
            volume = item[6]

            # --- Rule 1: Price Consistency Check ---
            try:
                if float(price) <= 0:
                    logger.warning(f"DQ Violation: Invalid price for {coin_id} (Value: {price})")
                    is_valid = False
                    error_count += 1
            except (ValueError, TypeError):
                logger.error(f"DQ Violation: Non-numeric price detected for {coin_id}")
                is_valid = False
                error_count += 1

            # --- Rule 2: Volume Integrity Check ---
            try:
                if float(volume) <= 0:
                    logger.warning(f"DQ Violation: Zero or negative volume for {coin_id} (Value: {volume})")
                    is_valid = False
                    error_count += 1
            except (ValueError, TypeError):
                logger.error(f"DQ Violation: Non-numeric volume detected for {coin_id}")
                is_valid = False
                error_count += 1

        if is_valid:
            logger.info("Data Quality Status: PASSED (All records healthy)")
        else:
            logger.error(f"Data Quality Status: FAILED (Found {error_count} integrity issues)")
            
        return is_valid