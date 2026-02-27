import requests
from typing import Dict, Any, List
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from config.settings import settings
from src.utils.logger import logger

class CoingeckoClient:
    """
    คลาสสำหรับเชื่อมต่อกับ CoinGecko API 
    ออกแบบมาให้มีระบบ Resilience (ความทนทาน) เพื่อรับมือกับปัญหา Network หรือ API Rate Limit
    """
    
    def __init__(self):
        # กำหนดที่อยู่หลักของ API (Base URL)
        self.base_url = 'https://api.coingecko.com/api/v3'
        
        # ตั้งค่า Header สำหรับการส่งคำขอข้อมูล
        self.headers = {
            'accept': 'application/json',                     # บอก API ว่าเราต้องการข้อมูลเป็นรูปแบบ JSON
            'x-cg-demo-api-key': settings.COINGECKO_API_KEY   # ยืนยันตัวตนด้วย API Key จากไฟล์ settings
        }

    # ระบบ Retry Logic: ถ้าดึงข้อมูลไม่สำเร็จ โปรแกรมจะพยายามใหม่เองอัตโนมัติ
    @retry(
        stop=stop_after_attempt(settings.RETRY_COUNT),               # หยุดพยายามเมื่อครบจำนวนครั้งที่ตั้งค่าไว้
        wait=wait_exponential(multiplier=1, min=4, max=10),          # ใช้กลยุทธ์ Exponential Backoff เพื่อเลี่ยงการโดนแบน
        retry=retry_if_exception_type(requests.exceptions.RequestException), # ลองใหม่เฉพาะเมื่อเกิดปัญหาที่ตัว Network
        reraise=True                                                 # ส่ง Error ออกไปหากลองจนครบกำหนดแล้วยังพังอยู่
    )
    def get_coin_market(self, vs_currency: str = 'usd') -> List[Dict[str, Any]]:
        """
        ฟังก์ชันดึงข้อมูลราคาตลาด (Market Data) 
        """
        try:
            # แจ้งสถานะการเริ่มดึงข้อมูล 
            logger.info(f'Initiating data extraction from CoinGecko (Currency: {vs_currency})...')

            endpoint = f'{self.base_url}/coins/markets'
            
            # กำหนดเงื่อนไขการดึงข้อมูล (Query Parameters)
            params = {
                'vs_currency': vs_currency,
                'order': 'market_cap_desc',      # เรียงลำดับตามมูลค่าตลาด
                'per_page': settings.BATCH_SIZE, # จำนวนข้อมูลต่อหน้า (คุมปริมาณ Data)
                'page': 1                        # หน้าที่ต้องการดึง
            }

            # ส่งคำขอ HTTP GET
            response = requests.get(
                endpoint,
                headers=self.headers,
                params=params,
                timeout=settings.API_TIMEOUT # กำหนดเวลา Timeout ป้องกันโปรแกรมค้าง
            )

            # ตรวจสอบ HTTP Status: ถ้าพังจะโดดไปที่ส่วน Exception ทันที
            response.raise_for_status()

            logger.info('Successfully fetched market data from API.')
            return response.json()

        except Exception as e:
            # บันทึก Error หากกระบวนการดึงข้อมูลล้มเหลว
            logger.error(f'Critical error in data extraction: {str(e)}')
            return []