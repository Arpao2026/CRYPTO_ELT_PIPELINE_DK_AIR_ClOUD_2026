import pytest
from src.extractors.coingecko import CoingeckoClient
import requests

def test_extract_success(mocker):
    """
    ทดสอบว่า API ส่งข้อมูลกลับมาปกติ โค๊ดของเราต้องรับค่าได้ถูกต้อง
    """
    Client = CoingeckoClient()

    #1. สร้างข้อมูลปลอม (Mock Response) ที่หน้้าตาเหมือนของ CoinGecko
    mock_respone_data = [
    {"id": "bitcoin","symbol": "btc","name": "Bitcoin","current_price": 70187,"market_cap": 1381651251183,
   "total_volume": 20154184933,"last_updated": "2024-04-07T16:49:31.736Z",}
    ]

    #2.ทำการ 'สวมรวม' (Mock) ฟังก์ชั่น request.get ใน CoingeckoClient
    # เราบอกว่า 'ไม่ต้องไปเรียกเน็ตจริงนะ ให้ส่งค่า mock_respone_data กลับมาเลย
    mock_get =  mocker.patch('src.extractors.coingecko.requests.get')
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = mock_respone_data

    #3. รัน Code ของเราจริงๆ
    result = Client.get_coin_market()

    #4. ตรวจสอบผลลัพธ์ (Assertions)
    assert len(result) == 1
    assert result[0]['id'] == 'bitcoin'
    
    #ตรวจสอบด้วยว่าโค้ดของเราไปเรียก URL ถูกต้องไหม
    mock_get.assert_called_once()

def test_extract_api_error(mocker):
    """ 
    ทดสอบว่าถ้า API ล่ม (Error 500) โค้ดของเราจะคืนค่าเป็นลิสต์ว่าง [] ไม่ใช่ทำโปรเเกรมพัง

    """
    client = CoingeckoClient()

    # เราจะ Mock ที่ requests.get เหมือนเดิม
    # แต่เราจะบังคับให้มันคืนค่า Response ทีมี status_code = 500
    mock_get = mocker.patch('src.extractors.coingecko.requests.get')

    # สร้าง Mock Respone ปลอมๆ ขึ้นมา
    mock_respone = mocker.Mock()
    mock_respone.status_code = 500
    mock_respone.raise_for_status.side_effect = requests.exceptions.HTTPError('500 server Error')
  
    mock_get.return_value = mock_respone

    # รันโค้ด
    result = client.get_coin_market()

    # ตรวจสอบผลลัพธ์: ต้องได้ []
    assert result == []
    