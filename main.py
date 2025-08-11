import asyncio
import logging
from time import sleep

from clients.digiseller_client import DigisellerClient
from clients.google_sheets_client import GoogleSheetsClient
from logic.processor import process_single_payload
from services.sheet_service import SheetService
from utils.config import settings


async def run_automation():
    try:
        g_client = GoogleSheetsClient(settings.GOOGLE_KEY_PATH)
        sheet_service = SheetService(client=g_client)

        payloads_to_process = sheet_service.get_payloads_to_process()

        if not payloads_to_process:
            logging.info("Không có hàng nào cần xử lý. Kết thúc chương trình.")
            return
        async with DigisellerClient() as client:
            for payload in payloads_to_process:
                try:
                    hydrated_payload = sheet_service.fetch_data_for_payload(payload)
                    result = await process_single_payload(client, hydrated_payload)

                    sheet_service.update_log_for_payload(payload, result)
                    logging.info(
                        f"Processed row {payload.row_index} ({payload.product_name}), sleeping for "
                        f"{settings.SLEEP_TIME} seconds.")
                    sleep(settings.SLEEP_TIME)

                except Exception as e:
                    logging.error(f"Error in flow: {payload.row_index} ({payload.product_name}): {e}")
                    error_result = {'note': f"Error: {e}"}
                    sheet_service.update_log_for_payload(payload, error_result)
                    logging.error(f"Row {payload.row_index} ({payload.product_name}) due to error: {e}, sleeping for "
                                  f"{settings.SLEEP_TIME} seconds.")
                    sleep(settings.SLEEP_TIME)


    except Exception as e:
        logging.critical(f"Đã xảy ra lỗi nghiêm trọng, chương trình dừng lại: {e}", exc_info=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.getLogger("httpx").setLevel(logging.ERROR)
    logging.getLogger("httpcore").setLevel(logging.ERROR)

    while True:
        asyncio.run(run_automation())
        logging.info("Completed processing all payloads. Next round in 10 seconds.")
        sleep(10)
