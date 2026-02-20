import pytest
from src.quality.data_quality import DataqualityValidator

def test_validator_datects_negative_price():
    validator = DataqualityValidator()

    #จำลองข้อมูลที่สะอาดแล้ว แต่มีเหรียญหนึ่งที่ราคา "ติดลบ" (ซึ่งเป็นไปไม่ได้)
    bad_data = [
        ("test_batch_001","bitcoin", "btc", "Bitcoin", -50000.0, 1000000.0, 5000.0, "2026-02-17T00:00:00Z")
    ]

    # ผลลัพธ์ที่คาดหวัง: Validator ต้องส่งกลับมาเป็น False (ไม่ผ่าน)
    assert validator.validate_market_data(bad_data) is False

def test_validator_passes_good_data():
    validator = DataqualityValidator()

    #ข้อมูลปกติ
    good_data = [
        ("test_batch_001","bitcoin", "btc", "Bitcoin", 50000.0, 1000000.0, 5000.0, "2026-02-17T00:00:00Z")
    ]

    #ผลลัพธ์ที่คาดหวัง: ต้องผ่าน (True)
    assert validator.validate_market_data(good_data) is True