# 1. ใช้ Python 3.9 แบบเบา
FROM python:3.9-slim

# 2. กำหนดพื้นที่ทำงานในกล่อง
WORKDIR /app

# 3. ก๊อปปี้ไฟล์ติดตั้ง Library
COPY requirements.txt .

# 4. ติดตั้ง Library ตามที่คุณ Freeze มา
RUN pip install --no-cache-dir -r requirements.txt

# 5. ก๊อปปี้ทุกอย่างในโปรเจกต์เข้าไป (src, config, main.py)
COPY . .

# 6. ตั้งค่าให้ Python มองเห็นโฟลเดอร์ src
ENV PYTHONPATH=/app

# 7. สั่งรันโปรเจกต์
CMD ["python", "main.py"]