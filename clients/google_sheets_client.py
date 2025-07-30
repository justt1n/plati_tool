# clients/google_sheets_client.py
import logging
from typing import List

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class GoogleSheetsClient:
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    def __init__(self, key_path: str):
        try:
            creds = service_account.Credentials.from_service_account_file(key_path, scopes=self.SCOPES)
            self.service = build('sheets', 'v4', credentials=creds)
            logging.info("Đã kết nối thành công tới Google Sheets API.")
        except FileNotFoundError:
            logging.error(
                f"Không tìm thấy file key tại: '{key_path}'. Vui lòng kiểm tra lại đường dẫn trong file settings.env.")
            raise
        except Exception as e:
            logging.error(f"Lỗi khi khởi tạo GoogleSheetsClient: {e}")
            raise

    def get_data(self, spreadsheet_id: str, range_name: str) -> List[List[str]]:
        try:
            result = self.service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, range=range_name
            ).execute()
            values = result.get('values', [])
            # logging.info(f"Đã lấy thành công {len(values)} hàng từ dải ô '{range_name}'.")
            return values
        except HttpError as error:
            logging.error(f"Đã xảy ra lỗi API khi lấy dữ liệu: {error}")
            return []

    def batch_update(self, spreadsheet_id: str, data: List[dict]):
        try:
            body = {'data': data, 'valueInputOption': 'USER_ENTERED'}
            result = self.service.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id, body=body
            ).execute()
            logging.info(f"{result.get('totalUpdatedCells')} ô đã được cập nhật.")
        except HttpError as error:
            logging.error(f"Đã xảy ra lỗi API khi cập nhật dữ liệu: {error}")
