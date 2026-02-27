from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path

# 1. Project Root Directory: กำหนดตำแหน่งหลักของโปรเจกต์ เพื่อให้เรียกใช้ Path ต่างๆ ได้แม่นยำไม่ว่าจะรันจากโฟลเดอร์ไหน
BASE_DIR = Path(__file__).resolve().parent.parent

class Settings(BaseSettings):
    """
    คลาสสำหรับจัดการ Configuration ทั้งหมดของโปรเจกต์ 
    โดยใช้ Pydantic เพื่อทำ Data Validation (ตรวจสอบความถูกต้องของข้อมูล) ตั้งแต่เริ่มรันโปรแกรม
    """
    
    # 2. Define fields with correct Type Hints
    # บรรทัดที่ไม่มีเครื่องหมาย = คือ "Required Field" (ต้องมีใน .env เท่านั้น)
    # ช่วยป้องกันปัญหา 'Key Error' กลางทางขณะที่โปรแกรมกำลังทำงาน
    COINGECKO_API_KEY: str  
    DATABASE_URL: str  

    # บรรทัดที่มีเครื่องหมาย = คือการกำหนด 'Default Value' 
    # ทำให้ระบบยังทำงานได้แม้จะไม่ได้ตั้งค่าไว้ใน Environment
    API_TIMEOUT: int = 30 
    RETRY_COUNT: int = 5
    LOG_LEVEL: str = 'INFO' 
    BATCH_SIZE: int = 100

    # 3. Pydantic Configuration: ตั้งค่าการอ่านไฟล์ .env
    model_config = SettingsConfigDict(
        # เชื่อมโยงกับไฟล์ .env ที่อยู่ส่วนกลางของโปรเจกต์
        env_file=BASE_DIR / '.env',  
        env_file_encoding='utf-8',
        # 'ignore' หมายถึงถ้ามีตัวแปรอื่นใน .env ที่เราไม่ได้ระบุไว้ในนี้ ให้ข้ามไป (ช่วยให้ระบบยืดหยุ่น)
        extra = 'ignore'  
    )

# 4. Instantiate: สร้างตัวแปร settings เพื่อให้ไฟล์อื่นเรียกใช้งานได้ทันที (Singleton Pattern)
settings = Settings()