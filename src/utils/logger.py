import logging
import sys
from pathlib import Path 

def setup_logger():
    #  สร้าง Folder logs ถ้ายังไม่มี
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok= True)

    #ตั้งค่ารูปแบบของ Log (เวลา | ระดับความรุนแรง | ข้อความ)
    log_format = "%(asctime)s - %(name)s = %(levelname)s - %(message)s"

    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler('logs/pipeline.log'), #บันทึกลงไฟล์
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger('cryptoPipeline')
logger = setup_logger()