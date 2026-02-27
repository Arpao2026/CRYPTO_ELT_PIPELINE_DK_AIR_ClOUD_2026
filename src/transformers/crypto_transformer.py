import sqlite3
import json
import os
from typing import List, Dict, Any, Tuple
from config.settings import settings
from src.utils.logger import logger

class CryptoTransformer:
    """
    จัดการขั้นตอน Transformation และ Core Loading (T & L ในกระบวนการ ETL)
    รับผิดชอบการใส่กฎทางธุรกิจ (Business Logic), การกรองข้อมูล และการบันทึกข้อมูลที่ถูกคัดออก (Auditing)
    """
    
    def __init__(self):
        # ดึงที่อยู่ฐานข้อมูลจากระบบ Config ส่วนกลาง
        self.db_path = settings.DATABASE_URL.replace('sqlite:///', '').strip()
    
    def get_cleaned_data(self, batch_id: str = None) -> List[Tuple]:
        """
        ดึงข้อมูลดิบจาก Staging และเริ่มกระบวนการแปลงข้อมูล (Transformation)
        
        Args:
            batch_id (str, optional): รหัสชุดข้อมูลที่ต้องการประมวลผล 
            หากไม่ระบุจะดึงข้อมูลทั้งหมดที่มีใน Staging
        """
        try:
            logger.info(f"Extracting raw data for processing (Batch: {batch_id})")

            # กลไก Incremental Loading: เลือกดึงข้อมูลเฉพาะรหัส Batch ที่ระบุเท่านั้น
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
            
            # ส่งข้อมูลดิบที่ดึงมาได้ไปประมวลผลต่อที่ฟังก์ชัน transform_logic
            return self.transform_logic(rows, batch_id)

        except Exception as e:
            logger.error(f"Failed to extract data from Staging: {str(e)}")
            return []
    
    def transform_logic(self, rows: List[Tuple], batch_id: str = None) -> List[Tuple]:
        """ 
        ถอดรหัส JSON และนำกฎทางธุรกิจมาประยุกต์ใช้เพื่อกรองข้อมูลที่ไม่สมบูรณ์ออก
        """
        cleaned_data = []
        data_issues = [] 
        
        for row in rows:
            try:
                # แปลงข้อมูลจาก JSON String กลับมาเป็น Dictionary (Deserialization)
                item = json.loads(row[0])
                price = item.get('current_price', 0)
                volume = item.get('total_volume', 0)

                # ออกแบบโครงสร้างข้อมูลใหม่ (Mapping) ให้ตรงกับตาราง Fact Table หลัก
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

                # กฎการแปลงข้อมูล (Transformation Rule): เก็บเฉพาะเหรียญที่มีการซื้อขายจริง (ราคาและวอลลุ่ม > 0)
                if price > 0 and volume > 0:
                    cleaned_data.append(tuple(record.values()))
                else:
                    # เก็บข้อมูลที่ถูกคัดออกเพื่อใช้ในการตรวจสอบสาเหตุภายหลัง (Data Quality Auditing)
                    record['reason'] = 'Invalid asset: Zero price or volume detected'
                    data_issues.append(record)
                    
            except (json.JSONDecodeError, TypeError) as e:
                logger.error(f"Data corruption detected - JSON Parsing Error: {e}")
                continue

        # หากพบข้อมูลที่มีปัญหา ให้ทำการบันทึกลงไฟล์ JSON เพื่อความโปร่งใสของข้อมูล (Observability)
        if data_issues:
            self.save_issues_to_json(data_issues, batch_id)

        logger.info(f"Transformation complete: {len(cleaned_data)} valid, {len(data_issues)} rejected.")
        return cleaned_data 
        
    def save_issues_to_json(self, issues: List[Dict], batch_id: str = None):
        """ 
        บันทึกข้อมูลที่ไม่ผ่านเกณฑ์ลงไฟล์ JSON เพื่อทำ Root Cause Analysis (RCA)
        โดยเก็บแยกตามโฟลเดอร์และรหัส Batch
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
        นำเข้าข้อมูลที่ถูกทำความสะอาดแล้วเข้าสู่ Fact Table หลัก
        ใช้รูปแบบการบันทึกแบบจัดเก็บประวัติ (Time-series) ด้วย Composite Primary Key
        """
        if not data:
            logger.warning("Ingestion skipped: No valid records to load.")
            return
        
        try:
            # สร้างตาราง Fact Table หากยังไม่มี (Schema นี้เน้นความเร็วในการวิเคราะห์ข้อมูล)
            # ใช้ (batch_id, coin_id) เป็น Primary Key เพื่อป้องกันข้อมูลซ้ำซ้อนในรอบการรันเดียวกัน
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
            
            # คำสั่งบันทึกข้อมูลแบบ Bulk พร้อมกลไก 'OR IGNORE' เพื่อรองรับคุณสมบัติ Idempotency (รันซ้ำได้ไม่พัง)
            insert_query = '''
            INSERT OR IGNORE INTO fct_crypto_prices
            (batch_id, coin_id, symbol, name, price, market_cap, total_volume, last_updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            '''

            with sqlite3.connect(self.db_path) as conn:
                conn.execute(create_table_query)
                # บันทึกข้อมูลปริมาณมากในครั้งเดียวเพื่อประสิทธิภาพสูงสุด
                conn.executemany(insert_query, data)
                conn.commit()
                logger.info(f"DATABASE LOAD SUCCESS: {len(data)} records archived in Fact Table.")
        
        except Exception as e:
            logger.error(f"Core Layer Load Error: {str(e)}")
            raise