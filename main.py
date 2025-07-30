# main.py
import logging

# Import các thành phần đã được tách biệt
from utils.config import settings
from clients.google_sheets_client import GoogleSheetsClient
from services.sheet_service import SheetService
from logic.processor import process_single_payload


def run_automation():
    """
    Hàm điều phối chính, kết nối các lớp với nhau.
    """
    try:
        # 1. Khởi tạo các thành phần cơ sở
        g_client = GoogleSheetsClient(settings.GOOGLE_KEY_PATH)
        sheet_service = SheetService(client=g_client)

        # 2. Lấy danh sách công việc từ Service
        payloads_to_process = sheet_service.get_payloads_to_process()

        if not payloads_to_process:
            logging.info("Không có hàng nào cần xử lý. Kết thúc chương trình.")
            return

        # 3. Đưa từng công việc cho lớp Logic xử lý và cập nhật kết quả
        for payload in payloads_to_process:
            try:
                # Gọi lớp Logic để thực hiện nghiệp vụ
                result = process_single_payload(payload)

                # Gọi lớp Service để ghi lại kết quả
                sheet_service.update_log_for_payload(payload, result)

            except Exception as e:
                # Nếu một hàng bị lỗi, ghi log và tiếp tục
                logging.error(f"Lỗi nghiêm trọng khi xử lý hàng {payload.row_index} ({payload.product_name}): {e}")
                error_result = {'note': f"Lỗi: {e}"}
                sheet_service.update_log_for_payload(payload, error_result)

        logging.info("Hoàn tất tất cả các tác vụ.")

    except Exception as e:
        logging.critical(f"Đã xảy ra lỗi nghiêm trọng, chương trình dừng lại: {e}", exc_info=True)


if __name__ == "__main__":
    # Cấu hình logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Chạy hàm điều phối
    run_automation()