# digiseller_service.py
import logging
from typing import List

from clients.digiseller_client import DigisellerClient
from models.digiseller_models import SellerItem

logger = logging.getLogger(__name__)


async def get_all_items() -> List[SellerItem]:
    """
    Fetches all items from a seller by handling API pagination automatically.

    This service function repeatedly calls the `get_seller_items` method
    for each page until all items have been retrieved.

    Returns:
        A single flat list containing all SellerItem objects.
    """
    async with DigisellerClient() as client:
        all_items: List[SellerItem] = []
        current_page = 1
        total_pages = 1

        logging.info("Starting to fetch all seller items, this may take a while...")

        while current_page <= total_pages:
            logging.info(f"Fetching page {current_page}/{total_pages}...")

            response = await client.get_seller_items(page=current_page, rows=1000)  # Lấy tối đa mỗi lần

            if response:
                total_pages = response.total_pages

                if response.items:
                    all_items.extend(response.items)
                else:
                    break
            else:
                break

            current_page += 1

        logging.info(f"Finished fetching. Total items found: {len(all_items)}")
        return all_items