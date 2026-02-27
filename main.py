from src.extractors.coingecko import CoingeckoClient
from src.loaders.sqlite_loader import SQLiteLoader
from src.transformers.crypto_transformer import CryptoTransformer
from src.quality.data_quality import DataqualityValidator
from src.utils.logger import logger

def run_pipeline():
    """
    ทำหน้าที่เป็น Orchestrator ควบคุมการทำงานของกระบวนการ ELT ทั้งหมดแบบครบวงจร (End-to-End)
    ลำดับขั้นตอน: Extract (API) -> Load (Staging) -> Transform (Logic) -> DQ (Audit) -> Load (Core)
    """
    logger.info('--- Initiating Cryptocurrency ELT Pipeline ---')

    # ขั้นตอนที่ 1: Extraction Phase (การดึงข้อมูลจากแหล่งต้นทาง)
    # เรียกใช้งาน Client ที่มีระบบ Retry Logic (Tenacity) ในตัวเพื่อความเสถียร
    client = CoingeckoClient()
    raw_data = client.get_coin_market()

    if not raw_data:
        # หากดึงข้อมูลไม่ได้ ให้หยุดการทำงานของ Pipeline ทันทีเพื่อความปลอดภัย
        logger.error('Pipeline Aborted: No data retrieved from API.')
        return

    # ขั้นตอนที่ 2: Loading Phase (การเก็บข้อมูลลง Staging / Data Lake)
    # บันทึกข้อมูลดิบในรูปแบบ JSON เพื่อใช้สำหรับการตรวจสอบย้อนหลัง (Traceability)
    loader = SQLiteLoader()
    current_batch_id = loader.load_to_staging(raw_data)

    # ขั้นตอนที่ 3: Transformation Phase (การประมวลผลและคัดกรอง)
    # ดึงข้อมูลจาก Staging ตาม Batch ล่าสุด และประยุกต์ใช้กฎทางธุรกิจ (Business Rules)
    transformer = CryptoTransformer()
    cleaned_data = transformer.get_cleaned_data(batch_id=current_batch_id)

    # ขั้นตอนที่ 4: Data Quality Validation (การตรวจคุณภาพก่อนเข้าฐานข้อมูลจริง)
    # ทำหน้าที่เป็น Gatekeeper ตรวจสอบความถูกต้องของข้อมูล (Data Integrity) เช่น ราคาต้อง > 0
    dq = DataqualityValidator()
    
    # ตรวจสอบว่าข้อมูลใน Batch นี้ผ่านมาตรฐานคุณภาพหรือไม่
    if dq.validate_market_data(cleaned_data):
        # ขั้นตอนที่ 5: Final Load Phase (การนำข้อมูลเข้าสู่ Data Warehouse / Core Fact Table)
        # หากผ่านการตรวจ DQ ให้บันทึกข้อมูลที่ผ่านการขัดเกลาแล้วลงในตาราง Production
        transformer.save_to_core(cleaned_data)
        logger.info('--- Pipeline Execution Completed Successfully ---')
    else:
        # หาก DQ ไม่ผ่าน: สั่งหยุดการทำงานเพื่อป้องกันข้อมูลที่ผิดพลาดหลุดเข้าสู่ระบบ Core
        logger.error('Pipeline Halted: Data Quality validation failed. Ingestion cancelled.')

# จุดเริ่มต้นของการรันโปรแกรม
if __name__ == '__main__':
    run_pipeline()