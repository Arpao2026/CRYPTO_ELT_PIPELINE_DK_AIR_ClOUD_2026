import requests
from typing import Dict, Any, List
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from config.settings import settings
from src.utils.logger import logger

class CoingeckoClient:
    """
    Client for interacting with the CoinGecko API.
    Handles data extraction with built-in resilience and retry logic.
    """
    
    def __init__(self):
        # Base URL for CoinGecko API v3
        self.base_url = 'https://api.coingecko.com/api/v3'
        
        # Authentication and configuration headers
        self.headers = {
            'accept': 'application/json',
            'x-cg-demo-api-key': settings.COINGECKO_API_KEY
        }

    @retry(
        # Stops retrying after a pre-configured number of attempts in settings
        stop=stop_after_attempt(settings.RETRY_COUNT),
        # Implements Exponential Backoff (waits 4s, 8s, etc.) to respect API rate limits
        wait=wait_exponential(multiplier=1, min=4, max=10),
        # Only retries on Network-related errors (e.g., Connection Timeout, 5xx errors)
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        # Ensures the final exception is raised if all retry attempts fail
        reraise=True
    )
    def get_coin_market(self, vs_currency: str = 'usd') -> List[Dict[str, Any]]:
        """
        Fetches current market data for cryptocurrencies.
        
        Args:
            vs_currency (str): The target currency to compare prices against. Defaults to 'usd'.
            
        Returns:
            List[Dict[str, Any]]: A list of cryptocurrency market data objects.
            Returns an empty list if an exception occurs after exhausting retries.
        """
        try:
            logger.info(f'Fetching market data from CoinGecko (Currency: {vs_currency})...')

            endpoint = f'{self.base_url}/coins/markets'
            
            # API Query Parameters
            params = {
                'vs_currency': vs_currency,
                'order': 'market_cap_desc',     # Rank by Market Capitalization
                'per_page': settings.BATCH_SIZE, # Records per page
                'page': 1                       # Page number
            }

            # Execute HTTP GET request
            response = requests.get(
                endpoint,
                headers=self.headers,
                params=params,
                timeout=settings.API_TIMEOUT # Prevents the script from hanging on slow responses
            )

            # Raise an HTTPError if the response status is 4xx or 5xx
            response.raise_for_status()

            logger.info('Successfully fetched market data from API.')
            return response.json()

        except Exception as e:
            # Logs the error and returns an empty list to prevent pipeline failure
            logger.error(f'Critical error in data extraction: {str(e)}')
            return []