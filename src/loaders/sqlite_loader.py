import sqlite3
import json
from typing import List, Dict, Any, Tuple
from datetime import datetime
from config.settings import settings
from src.utils.logger import logger

class SQLiteLoader:
    """
    จัดการขั้นตอน 'Load' ของกระบวนการ ELT โดยการบันทึกข้อมูลดิบ (Raw Data)
    ลงใน SQLite staging area ซึ่งเปรียบเสมือน Data Lake ของโปรเจกต์
    """
    
    def __init__(self):
        # ดึงที่อยู่ไฟล์ฐานข้อมูลโดยการตัด prefix 'sqlite:///' ออก
        self.db_path = settings.DATABASE_URL.replace('sqlite:///', '').strip()
        try:
            # แจ้งสถานะการเริ่มต้นฐานข้อมูล Staging (Log เป็นภาษาอังกฤษ)
            logger.info(f"Initializing SQLite staging at: {self.db_path}")
            # ตรวจสอบและสร้างตาราง Staging ทันทีเมื่อเริ่มใช้งาน Class
            self._create_staging_table()
        except Exception as e:
            logger.error(f"Failed to initialize SQLiteLoader: {str(e)}")
            raise

    def _create_staging_table(self):
        """
        สร้างตาราง Staging เพื่อเก็บข้อมูลดิบในรูปแบบ JSON ที่ไม่ถูกแก้ไข (Immutable)
        มีการเก็บ batch_id เพื่อใช้ในการตรวจสอบที่มาของข้อมูล (Traceability) และการตรวจสอบ (Auditing)
        """
        query = '''
        CREATE TABLE IF NOT EXISTS stg_crypto_markets(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            coin_id TEXT,               -- รหัสอ้างอิงของเหรียญ (เช่น 'bitcoin')
            raw_data TEXT,              -- ข้อมูล JSON ทั้งก้อนที่ได้รับจาก API เก็บในรูปแบบ String
            extracted_at TIMESTAMP,     -- วันเวลาที่ดึงข้อมูลออกมา
            batch_id TEXT               -- รหัสชุดข้อมูลเพื่อใช้ระบุรอบการรันของ Pipeline
        );
        '''
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(query)
            logger.debug("Staging table 'stg_crypto_markets' is ready.")

    def load_to_staging(self, data: List[Dict[str, Any]]) -> str:
        """
        บันทึก List ของ Dictionary ลงในตาราง Staging โดยใช้เทคนิค Bulk Insertion
        
        Args:
            data (List[Dict[str, Any]]): ข้อมูลดิบที่ได้รับมาจาก API
            
        Returns:
            str: รหัส batch_id ที่สร้างขึ้นสำหรับรอบการโหลดข้อมูลนี้
        """
        try:
            # สร้าง batch_id และเวลาที่ดึงข้อมูล เพื่อใช้ติดตามข้อมูลรายรอบ (Incremental Loading)
            batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')
            extracted_at = datetime.now().isoformat()

            # ใช้ Parameterized SQL เพื่อป้องกันช่องโหว่ SQL Injection
            insert_query = ''' 
            INSERT INTO stg_crypto_markets (coin_id, raw_data, extracted_at, batch_id)
            VALUES (?, ?, ?, ?)
            '''
            
            # เตรียมข้อมูลสำหรับการทำ รูปแบบ List of Tuples
            rows_to_insert = [
                (item.get('id'), json.dumps(item), extracted_at, batch_id)
                for item in data
            ]

            # ดำเนินการ Insert ข้อมูลปริมาณมากภายในหนึ่ง Transaction
            with sqlite3.connect(self.db_path) as conn:
                conn.executemany(insert_query, rows_to_insert)
                conn.commit()
                # บันทึกสถานะการโหลดข้อมูลสำเร็จ 
                logger.info(f"Successfully loaded {len(rows_to_insert)} records to Staging (Batch: {batch_id})")

            # ส่งค่า batch_id กลับไปเพื่อให้ขั้นตอน Transform ใช้งานต่อได้ถูกต้อง
            return batch_id

        except Exception as e:
            logger.error(f"Load to staging failed: {str(e)}")
            raise