import asyncio  # <-- Thêm import
import logging
from typing import List

from clients.digiseller_client import DigisellerClient
from logic.processor import consolidate_price_updates
from models.digiseller_models import ProductPriceUpdate


class PriceUpdateBatcher:
    """
    A class to batch product price updates and send them in bulk.
    It acts as a context manager to ensure the final batch is always sent.
    """

    def __init__(self, client: DigisellerClient, batch_size: int):
        """
        Initializes the batch processor.

        Args:
            client: The DigisellerClient instance to use for API calls.
            batch_size: The number of updates to collect before sending.
        """

        if batch_size == 1:
            batch_size = 20

        if batch_size < 1:
            raise ValueError("Batch size must be at least 1.")
        self.client = client
        self.batch_size = batch_size
        self._batch: List[ProductPriceUpdate] = []
        self._lock = asyncio.Lock()  # <-- Thêm Lock

    async def __aenter__(self):
        """Enter the async context, returning self."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the async context, flushing any remaining items."""
        logging.info("Exiting batcher context, flushing final batch...")
        await self.flush()

    async def add(self, item: ProductPriceUpdate):
        """
        Adds an item to the batch and flushes the batch if it's full.
        """
        self._batch.append(item)
        if len(self._batch) >= self.batch_size:
            # Không cần lock ở đây, vì flush() đã được bảo vệ
            await self.flush()

    async def flush(self):
        """
        Sends all items currently in the batch to the API.
        This operation is serialized by a lock to prevent queue limit errors.
        """
        async with self._lock:
            if not self._batch:
                return

            batch_to_send = self._batch.copy()
            self._batch.clear()

            logging.info(f"Flushing batch of {len(batch_to_send)} price updates...")
            try:
                batch_to_send = consolidate_price_updates(batch_to_send)
                response = await self.client.bulk_update_prices(batch_to_send)
                if response.taskId:
                    logging.info(f"Batch update successful. Task ID: {response.taskId}")
                else:
                    logging.error(f"Batch update failed: {response.return_description}")
            except Exception as e:
                logging.error(f"An error occurred during batch flush: {e}", exc_info=True)
                self._batch.extend(batch_to_send)  # Tùy chọn: Thử lại sau
