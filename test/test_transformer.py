import pytest
import json
from src.transformers.crypto_transformer import CryptoTransformer

def test_filtering_logic():
    """
    ทดสอบว่าระบบต้องกรองเหรียญที่ราคาหรือ volume เป็น 0 ออก
    """
    transformer = CryptoTransformer()
    batch_test_id = "test_batch_001"
    # จำลองข้อมูลดิบ (Mock Data)
    # 1. เหรียญดี, 2. เหรียญราคา 0, 3. เหรียญ Volume 0
    mock_raw_data = [
        (json.dumps({"id": "bitcoin", "current_price": 50000, "total_volume": 1000, "symbol": "btc", "name": "Bitcoin"}),),
        (json.dumps({"id": "zero-price", "current_price": 0, "total_volume": 1000, "symbol": "zp", "name": "Zero Price"}),),
        (json.dumps({"id": "zero-vol", "current_price": 50000, "total_volume": 0, "symbol": "zv", "name": "Zero Vol"}),)
    ]
    cleaned_data = transformer.transform_logic(mock_raw_data, batch_id=batch_test_id)
    assert len(cleaned_data) == 1
    assert cleaned_data[0][0] == batch_test_id
    assert cleaned_data[0][1] == 'bitcoin'

    def test_transform_handles_dirty_data():
        """
        ทดสอบการจัดการข้อมูลทีสกปรก (Dirty Data) เช่น ค่า Null หรือ String ในฟิลด์ตัวเลข
        """
        transformer = CryptoTransformer()
        batch_test_id = "dirty_batch_001"

    # จำลองข้อมูลที่มักจะทำให้ Pipeline พัง
        mock_dirty_data = [
            # 1. ราคาเป็น null (None)
            (json.dumps({"id": "null-price", "current_price": None, "total_volume": 1000, "symbol": "np"}),),
            # 2. ราคาเป็น String (ถ้าโค้ดไม่ใช้ float() จะพังตรงนี้)
            (json.dumps({"id": "string-price", "current_price": "50000", "total_volume": 1000, "symbol": "sp"}),),
            # 3. ข้อมูลหายไปเลย (Missing keys)
            (json.dumps({"id": "missing-data", "symbol": "md"}),)
        ]
        cleaned_data = transformer.transform_logic(mock_dirty_data,batch_id=batch_test_id)

        #ผลลัพธ์: ควรจะจัดการได้โดยไม่พ่น Exception ออกมา (แต่อาจจะถูกกรองออกถ้าค่าเป็น 0)
        assert isinstance(cleaned_data, list)