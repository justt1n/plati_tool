# services/sheet_service.py
import logging
from collections import defaultdict
from typing import List, Optional, Dict, Any

from clients.google_sheets_client import GoogleSheetsClient
from models.sheet_models import Payload
from utils.config import settings


def _find_header_row(rows: List[List[str]], key_columns: List[str]) -> Optional[int]:
    """Find the index of the header row in the provided rows."""
    for i, row in enumerate(rows):
        if all(key in row for key in key_columns):
            # logging.info(f"Found header row at index {i} with columns: {row}")
            return i
    return None


class SheetService:

    def __init__(self, client: GoogleSheetsClient):
        self.client = client

    def get_payloads_to_process(self) -> List[Payload]:
        all_rows = self.client.get_data(settings.MAIN_SHEET_ID, settings.MAIN_SHEET_NAME)
        if not all_rows:
            logging.warning("No data found in the main sheet.")
            return []

        header_row_index = _find_header_row(all_rows, settings.HEADER_KEY_COLUMNS)
        if header_row_index is None:
            logging.error(f"Cannot find header row with columns: {settings.HEADER_KEY_COLUMNS}")
            logging.error("Please check the header row in your Google Sheet.")
            return []

        data_rows = all_rows[header_row_index + 1:]
        start_row_on_sheet = header_row_index + 2
        logging.info(f"Starting from index {header_row_index + 1} (row {start_row_on_sheet} on sheet).")
        payload_list: List[Payload] = []
        for i, row_data in enumerate(data_rows, start=start_row_on_sheet):
            payload = Payload.from_row(row_data, row_index=i)
            if payload and payload.is_check_enabled:
                payload_list.append(payload)

        logging.info(f"Found {len(payload_list)} payloads to process starting from row {start_row_on_sheet}.")
        return payload_list

    def update_log_for_payload(self, payload: Payload, log_data: Dict[str, Any]):
        try:
            update_request = payload.prepare_update(
                settings.MAIN_SHEET_NAME,
                log_data
            )
            if update_request:
                self.client.batch_update(settings.MAIN_SHEET_ID, update_request)
                logging.info(f"-> Successfully updated for row {payload.row_index} with data: {log_data}")
        except Exception as e:
            logging.error(f"Cannot update log for row {payload.row_index} ({payload.product_name}): {e}")

    def fetch_data_for_payload(self, payload: Payload) -> Payload:
        locations_to_fetch = {
            "min_price": payload.min_price_location,
            "max_price": payload.max_price_location,
            "stock": payload.stock_location,
            "black_list": payload.blacklist_location
        }

        #Create a map of requests by spreadsheet ID
        requests_by_spreadsheet = defaultdict(list)
        range_to_key_map = {}

        for key, loc in locations_to_fetch.items():
            # make sure the location is valid
            if loc and loc.sheet_id and loc.sheet_name and loc.cell:
                range_name = f"'{loc.sheet_name}'!{loc.cell}"
                requests_by_spreadsheet[loc.sheet_id].append(range_name)
                # map the range name to the key
                range_to_key_map[range_name] = key

        for sheet_id, ranges in requests_by_spreadsheet.items():
            fetched_values_map = self.client.batch_get_data(sheet_id, ranges)

            for response_range, value in fetched_values_map.items():
                key = range_to_key_map.get(response_range)

                if key and value is not None and value != '':
                    try:
                        if key == 'stock':
                            setattr(payload, f"fetched_{key}", int(value))
                        else:
                            setattr(payload, f"fetched_{key}", float(value))
                    except (ValueError, TypeError):
                        logging.warning(
                            f"Không thể chuyển đổi giá trị '{value}' cho '{key}' của payload {payload.row_index}")

        return payload
