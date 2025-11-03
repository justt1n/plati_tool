import asyncio
import logging
from typing import Optional, Dict, Any, List

import httpx

from clients.digiseller_client import DigisellerClient
from clients.google_sheets_client import GoogleSheetsClient
from logic.batcher import PriceUpdateBatcher
from logic.processor import process_single_payload
from models.sheet_models import Payload
from services.sheet_service import SheetService
from utils.config import settings


async def process_payload_worker(
        hydrated_payload: Payload,  # <-- Payload này đã được "nhồi" data
        batcher: PriceUpdateBatcher,
        http_client: httpx.AsyncClient
) -> Optional[Dict[str, Any]]:
    """
    Worker function: Xử lý logic cho MỘT payload.
    Retry logic đã được chuyển vào digiseller_service.py (nơi xảy ra request).
    """
    try:
        # --- THAY ĐỔI: Bỏ vỏ bọc tenacity ---
        # Gọi trực tiếp, vì retry đã ở bên trong service rồi
        logging.debug(f"Calling process_single_payload for row {hydrated_payload.row_index}")
        result = await process_single_payload(
            hydrated_payload,  # Tham số 1
            http_client  # Tham số 2
        )
        # --- KẾT THÚC THAY ĐỔI ---

        log_data = result.get('log_data')
        product_update = result.get('product_update')

        if product_update:
            await batcher.add(product_update)

        if log_data:
            return {'row_index': hydrated_payload.row_index, 'log_data': log_data}

    except Exception as e:
        # Lỗi này là lỗi cuối cùng SAU KHI service đã retry thất bại
        logging.error(f"Error in flow for row {hydrated_payload.row_index} (after retries): {e}")
        return {'row_index': hydrated_payload.row_index, 'log_data': {'note': f"Error (after retries): {e}"}}

    return None


def create_chunks(data: List[Any], chunk_size: int) -> List[List[Any]]:
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]


async def run_automation():
    try:
        g_client = GoogleSheetsClient(settings.GOOGLE_KEY_PATH)
        sheet_service = SheetService(client=g_client)
        payloads_to_process = sheet_service.get_payloads_to_process()

        if not payloads_to_process:
            logging.info("No payloads to process.")
            return

        payload_chunks = create_chunks(payloads_to_process, settings.WORKERS)

        logging.info(
            f"Processing {len(payloads_to_process)} payloads in {len(payload_chunks)} chunks of {settings.WORKERS}...")

        async with DigisellerClient() as client, \
                PriceUpdateBatcher(client=client, batch_size=settings.BATCH_SIZE) as batcher, \
                httpx.AsyncClient(timeout=10.0) as http_client:

            await client.get_valid_token()

            for i, chunk in enumerate(payload_chunks):
                logging.info(f"--- Processing Chunk {i + 1}/{len(payload_chunks)} ---")

                # 1. LẤY DATA HÀNG LOẠT
                logging.info(f"Batch-fetching data for chunk {i + 1}...")
                try:
                    hydrated_chunk = await sheet_service.fetch_data_for_payloads_chunk(chunk)
                except Exception as e:
                    logging.error(f"Failed to fetch data for chunk {i + 1}: {e}")
                    continue  # Bỏ qua chunk này nếu lỗi fetch

                # 2. Tạo danh sách các task VỚI PAYLOAD ĐÃ CÓ DATA
                tasks = [
                    process_payload_worker(hydrated_payload, batcher, http_client)
                    for hydrated_payload in hydrated_chunk
                ]

                # 3. Chạy song song các task XỬ LÝ
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # 4. Thu thập log_data từ kết quả
                log_updates = []
                for res in results:
                    if isinstance(res, Exception):
                        logging.error(f"Unhandled exception in worker: {res}")
                    elif res:
                        log_updates.append(res)

                # 5. Cập nhật log lên Google Sheet HÀNG LOẠT
                if log_updates:
                    logging.info(f"Batch updating {len(log_updates)} log entries for chunk {i + 1}...")
                    sheet_service.batch_update_logs(log_updates)

                # 6. Ngủ giữa các CHUNK
                if i < len(payload_chunks) - 1:
                    logging.info(f"Sleeping for {settings.SLEEP_TIME}s between chunks...")
                    await asyncio.sleep(settings.SLEEP_TIME)

    except Exception as e:
        if isinstance(e, asyncio.CancelledError):
            logging.warning("Automation run cancelled.")
        else:
            logging.critical(f"An unhandled error occurred in run_automation: {e}", exc_info=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.getLogger("httpx").setLevel(logging.ERROR)
    logging.getLogger("httpcore").setLevel(logging.ERROR)


    async def main_loop():
        try:
            while True:
                start_time = asyncio.get_event_loop().time()
                await run_automation()
                end_time = asyncio.get_event_loop().time()
                elapsed = end_time - start_time
                logging.info(f"Completed processing all payloads. Elapsed time: {elapsed:.2f} seconds.")
                logging.info(f"Next round in 10 seconds.")
                await asyncio.sleep(10)
        except KeyboardInterrupt:
            logging.info("Shutdown requested by user. Exiting.")
        except Exception as e:
            logging.error(f"Critical error in main_loop: {e}", exc_info=True)


    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        pass

    logging.info("Application shut down.")
