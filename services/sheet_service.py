# services/sheet_service.py
import logging
from typing import List, Optional, Dict, Any

from clients.google_sheets_client import GoogleSheetsClient
from models.sheet_models import Payload
from utils.config import settings


def _find_header_row(rows: List[List[str]], key_columns: List[str]) -> Optional[int]:
    """Quét qua các hàng để tìm chỉ số của hàng tiêu đề."""
    for i, row in enumerate(rows):
        if all(key in row for key in key_columns):
            # logging.info(f"Đã tìm thấy hàng tiêu đề tại chỉ số {i} (hàng {i + 1} trên sheet).")
            return i
    return None


class SheetService:
    """
    Lớp dịch vụ để đóng gói logic tương tác với Google Sheets.
    """

    def __init__(self, client: GoogleSheetsClient):
        self.client = client

    def get_payloads_to_process(self) -> List[Payload]:
        """
        Lấy và phân tích tất cả các hàng cần xử lý từ Google Sheet.
        """
        all_rows = self.client.get_data(settings.MAIN_SHEET_ID, settings.MAIN_SHEET_NAME)
        if not all_rows:
            logging.warning("Không có dữ liệu nào được trả về từ sheet.")
            return []

        header_row_index = _find_header_row(all_rows, settings.HEADER_KEY_COLUMNS)
        if header_row_index is None:
            logging.error(f"Không tìm thấy hàng tiêu đề chứa các cột: {settings.HEADER_KEY_COLUMNS}")
            return []

        data_rows = all_rows[header_row_index + 1:]
        start_row_on_sheet = header_row_index + 2
        logging.info(f"Bắt đầu từ hàng {start_row_on_sheet}.")
        payload_list: List[Payload] = []
        for i, row_data in enumerate(data_rows, start=start_row_on_sheet):
            payload = Payload.from_row(row_data, row_index=i)
            if payload and payload.is_check_enabled:
                payload_list.append(payload)

        logging.info(f"Đã tìm thấy {len(payload_list)} hàng được bật CHECK để xử lý.")
        return payload_list

    def update_log_for_payload(self, payload: Payload, log_data: Dict[str, Any]):
        """
        Cập nhật các ô log cho một payload cụ thể.
        """
        try:
            update_request = payload.prepare_update(
                settings.MAIN_SHEET_NAME,
                log_data
            )
            if update_request:
                self.client.batch_update(settings.MAIN_SHEET_ID, update_request)
                logging.info(f"-> Đã cập nhật thành công cho hàng {payload.row_index}.")
        except Exception as e:
            logging.error(f"Không thể cập nhật log cho hàng {payload.row_index}: {e}")
