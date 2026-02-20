from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path
import os

# 1. Project Root Directory
BASE_DIR = Path(__file__).resolve().parent.parent

class Settings(BaseSettings):
    # 2. Define fields with correct Type Hints
    COINGECKO_API_KEY: str      #บรรทัดนี้ไม่มีเครื่องหมาย = แปลว่า"ต้องมีข้อมูลนี้ใน .env เท่านั้น ถ้าไม่มีโปรเเกรมจะ Error ทันที (ป้องกันเราลืมใส่ API KEY)
    API_TIMEOUT: int = 30       #อันนี้มีค่า Default ให้ถ้าใน .env ไม่ได้เขียนไว้มันจะใช้ค่า 30 ให้อัตโนมัติ
    DATABASE_URL: str  
    RETRY_COUNT: int = 5
    LOG_LEVEL: str = 'INFO' 
    BATCH_SIZE : int = 100


    # 3. Pydantic Configuration
    model_config = SettingsConfigDict(
        env_file=os.path.join(BASE_DIR, '.env'),  #เข้าไปหยิบข้อมูลมาจากไฟล์ที่ชื่อว่า .env ที่อยู่ใน BASE_DIR นะ
        env_file_encoding='utf-8',
        extra = 'ignore'  #ถ้าในไฟล์ .env มีข้อมูลตัวอื่นที่ฉันไม่ได้เขียนไว้ใน Class นี้ก็ให้ข้ามๆ ไปไม่ต้องสนใจ
    )

# 4. Create the instance (Use lowercase 'settings' to avoid confusion with the class)
settings = Settings()