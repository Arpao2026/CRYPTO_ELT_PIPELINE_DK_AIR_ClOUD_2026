from src.utils.logger import logger

class DataqualityValidator:
    """
    ทำหน้าที่เป็น 'Gatekeeper' (ผู้คุมประตู) ขั้นสุดท้ายของ Pipeline
    เพื่อตรวจสอบความถูกต้องของข้อมูล (Data Integrity) ให้เป็นไปตามกฎธุรกิจก่อนจะบันทึกถาวร
    """
    def __init__(self):
        # ไม่มีการกำหนดค่าเริ่มต้นเป็นพิเศษ
        pass

    def validate_market_data(self, data: list) -> bool:
        """
        ดำเนินการตรวจสอบคุณภาพข้อมูล (Data Quality Checks) กับชุดข้อมูลที่ผ่านการ Transform มาแล้ว
        
        Args:
            data (list): ลิสต์ของ Tuple ที่บรรจุข้อมูลตลาดที่ผ่านการแปลงมาแล้ว
            ลำดับดัชนี (Schema Index): (batch_id[0], coin_id[1], symbol[2], name[3], 
            price[4], market_cap[5], volume[6], updated[7])
            
        Returns:
            bool: คืนค่า True หากข้อมูลทั้งชุดผ่านมาตรฐานคุณภาพ, คืนค่า False หากพบปัญหา
        """
        # ตรวจสอบเบื้องต้นว่ามีข้อมูลส่งเข้ามาให้ตรวจหรือไม่
        if not data:
            logger.warning('DQ Check: No data provided for validation.')
            return False
        
        # แจ้งสถานะการเริ่มตรวจสอบคุณภาพข้อมูล 
        logger.info(f'Executing Data Quality (DQ) checks for {len(data)} records...')
        
        is_valid = True
        error_count = 0

        for item in data:
            # ดึงค่าจากดัชนีของ Tuple ที่ได้จาก Transformer
            # หมายเหตุ: โครงสร้างข้อมูลใน Transformer จะมี batch_id อยู่ที่ลำดับ 0
            coin_id = item[1]
            price = item[4]
            volume = item[6]

            # --- กฎข้อที่ 1: ตรวจสอบความถูกต้องของราคา (Price Consistency) ---
            try:
                # ตรวจสอบว่าราคาต้องเป็นตัวเลขที่มากกว่า 0
                if float(price) <= 0:
                    logger.warning(f"DQ Violation: Invalid price for {coin_id} (Value: {price})")
                    is_valid = False
                    error_count += 1
            except (ValueError, TypeError):
                # กรณีข้อมูลราคาไม่ใช่ตัวเลข (เช่น เป็นค่าว่าง หรือ String ที่แปลงไม่ได้)
                logger.error(f"DQ Violation: Non-numeric price detected for {coin_id}")
                is_valid = False
                error_count += 1

            # --- กฎข้อที่ 2: ตรวจสอบความครบถ้วนของปริมาณการซื้อขาย (Volume Integrity) ---
            try:
                # ตรวจสอบว่าปริมาณการซื้อขายต้องมากกว่า 0
                if float(volume) <= 0:
                    logger.warning(f"DQ Violation: Zero or negative volume for {coin_id} (Value: {volume})")
                    is_valid = False
                    error_count += 1
            except (ValueError, TypeError):
                # กรณีข้อมูลวอลลุ่มไม่ใช่ตัวเลข
                logger.error(f"DQ Violation: Non-numeric volume detected for {coin_id}")
                is_valid = False
                error_count += 1

        # สรุปผลการตรวจสอบคุณภาพข้อมูลราย Batch
        if is_valid:
            logger.info("Data Quality Status: PASSED (All records healthy)")
        else:
            logger.error(f"Data Quality Status: FAILED (Found {error_count} integrity issues)")
            
        return is_valid