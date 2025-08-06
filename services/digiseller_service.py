# digiseller_service.py
import logging
from typing import List

from clients.digiseller_client import DigisellerClient
from clients.google_sheets_client import GoogleSheetsClient
from models.digiseller_models import SellerItem
from utils.config import settings

logger = logging.getLogger(__name__)

HEADER = [
    "ID", "Product Name", "Price", "Currency", "Price (USD)", "Price (RUR)", "Price (EUR)", "Price (UAH)",
    "In Stock", "Items in Stock", "Sales", "Returns", "Good Responses", "Bad Responses",
    "Sales (Hidden)", "Returns (Hidden)", "Good Responses (Hidden)", "Bad Responses (Hidden)",
    "Agent Commission", "Visible", "Has Discount", "Num Options", "Owner ID",
    "Release Date", "Info", "Additional Info", "Sale Info (JSON)"
]


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

            response = await client.get_seller_items(page=current_page, rows=1000)

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


async def items_to_sheet(items: List[SellerItem]) -> bool:
    """
    Writes a list of SellerItem objects to a specified Google Sheet,
    including all fields from the model. It overwrites any existing data.

    Args:
        items: A list of SellerItem objects to write to the sheet.

    Returns:
        True if the operation was successful, False otherwise.
    """
    if not items:
        logging.warning("No items to write to the sheet. Skipping.")
        return True

    logging.info(f"Preparing to write {len(items)} items with full details to Google Sheets...")

    data_to_write = [HEADER]
    for item in items:
        num_in_stock_str = str(item.num_in_stock) if item.num_in_stock is not None else ""
        info_str = item.info if item.info is not None else ""
        additional_info_str = item.additional_info if item.additional_info is not None else ""
        release_date_str = item.release_date if item.release_date is not None else ""

        sale_info_json = item.sale_info.model_dump_json()

        row = [
            item.id,
            item.name,
            item.price,
            item.currency,
            item.price_usd,
            item.price_rur,
            item.price_eur,
            item.price_uah,
            item.in_stock,
            num_in_stock_str,
            item.sales_count,
            item.returns_count,
            item.good_responses_count,
            item.bad_responses_count,
            item.sales_count_hidden,
            item.returns_count_hidden,
            item.good_responses_hidden,
            item.bad_responses_hidden,
            item.agent_commission,
            item.visible,
            item.has_discount,
            item.num_options,
            item.owner_id,
            release_date_str,
            info_str,
            additional_info_str,
            sale_info_json
        ]
        data_to_write.append(row)

    try:
        client = GoogleSheetsClient(settings.GOOGLE_KEY_PATH)
        spreadsheet_id = settings.EXPORT_SHEET_ID
        sheet_name = settings.EXPORT_SHEET_NAME

        logging.info(f"Clearing old data from sheet: '{sheet_name}'...")
        client.clear_sheet(spreadsheet_id, sheet_name)

        logging.info(f"Writing new data to sheet: '{sheet_name}'...")
        client.update_data(spreadsheet_id, f"'{sheet_name}'!A1", data_to_write)

        logging.info("Successfully wrote all items with full details to Google Sheets.")
        return True
    except Exception as e:
        logging.error(f"An error occurred while writing items to sheet: {e}", exc_info=True)
        return False

