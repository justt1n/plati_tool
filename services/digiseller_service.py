# digiseller_service.py
import asyncio
import logging
import re
from typing import List, Optional
from urllib.parse import quote_plus

import httpx
from bs4 import BeautifulSoup

from clients.digiseller_client import DigisellerClient
from clients.google_sheets_client import GoogleSheetsClient
from models.digiseller_models import SellerItem, BsProduct, InsideProduct
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


async def get_product_list(html_str: str, key_words: str) -> List[BsProduct]:

    if not html_str:
        logger.error("No HTML content provided to get product list.")
        return []

    soup = BeautifulSoup(html_str, 'html.parser')
    product_list_container = soup.find('ul', id='item_list')

    if not product_list_container:
        logger.warning("Could not find the product list container")
        return []

    product_cards = product_list_container.find_all('li', class_='section-list__item')

    initial_products = []
    for card in product_cards:
        product_link_tag = card.find('a', class_='card')
        if not product_link_tag:
            continue

        name = product_link_tag.get('title', 'No name').strip()
        relative_link = product_link_tag.get('href', '')

        if relative_link.startswith('//'):
            full_link = f"https:{relative_link}"
        elif relative_link.startswith('/'):
            full_link = f"https://plati.market{relative_link}"
        else:
            full_link = relative_link

        price_tag = product_link_tag.find('span', class_='title-bold')
        outside_price = price_tag.get_text(strip=True).replace('₽', '').replace('\xa0',
            ' ').strip() if price_tag else 'N/A'

        sold_tag = product_link_tag.find('span', string=lambda text: text and 'Sold' in text)
        sold_count = sold_tag.get_text(strip=True).replace('Sold', '').strip() if sold_tag else None

        img_tag = product_link_tag.find('img', class_='preview-image')
        img_url = img_tag.get('src', 'N/A') if img_tag else 'N/A'
        if img_url.startswith('//'):
            img_url = 'https:' + img_url

        product_instance = BsProduct(
            name=name,
            outside_price=outside_price,
            sold_count=sold_count,
            link=full_link,
            image_link=img_url,
            price=None
        )
        initial_products.append(product_instance)

    if not initial_products:
        return []

    async with httpx.AsyncClient() as client:
        price_tasks = [
            _get_inside_price(p.link, key_words, client) for p in initial_products
        ]
        prices = await asyncio.gather(*price_tasks, return_exceptions=True)

    for product, price_result in zip(initial_products, prices):
        if isinstance(price_result, float):
            product.price = price_result
        elif isinstance(price_result, Exception):
            logger.error(f"Failed to fetch price for {product.name} due to an exception: {price_result}")

    return initial_products


async def _get_inside_price(link: str, key_words: str, client: httpx.AsyncClient) -> float:
    try:
        response = await client.get(link)
        response.raise_for_status()
        html_content = response.text
        options = _extract_price_options_with_url(html_content)
        url_price = _find_option_url_by_keywords(options, key_words)

        if url_price:
            price_response = await client.get(url_price)
            price_response.raise_for_status()
            price_data = price_response.json()
            return float(price_data.get('price', 0.0))

    except Exception as e:
        logger.error(f"Error fetching inside price from {link}: {e}")

    return 0.0


def _find_option_url_by_keywords(options: List[InsideProduct], keywords_str: str) -> Optional[str]:
    keywords = [k.strip() for k in keywords_str.split(",")]
    for keyword in keywords:
        target_price = _normalize_price_string(keyword)
        if target_price is not None:
            for option in options:
                option_price = _normalize_price_string(option.price_text)
                if option_price is not None and abs(target_price - option_price) < 1e-9:
                    return option.request_url

        pattern = re.compile(r'\b' + re.escape(keyword) + r'\b', re.IGNORECASE)
        for option in options:
            if pattern.search(option.price_text):
                return option.request_url
    return None


def _extract_price_options_with_url(html_str: str, currency: str = 'USD') -> List[InsideProduct]:
    if not html_str:
        return []

    soup = BeautifulSoup(html_str, 'html.parser')
    options_data = []

    options_container = soup.find('div', class_='id_chips_container')
    if not options_container:
        return []

    price_chips = options_container.find_all('div', class_='chips--large')

    for chip in price_chips:
        input_tag = chip.find('input', class_='chips__input')
        label_tag = chip.find('label', class_='chips__label')

        if not (input_tag and label_tag):
            continue

        item_id = input_tag.get('data-item-id')
        option_id = input_tag.get('data-id')
        value_id = input_tag.get('value')

        price_text = label_tag.get_text(strip=True)

        currency_match = re.search(r'[A-Za-z]+', price_text)
        if currency_match:
            currency = currency_match.group(0).upper()

        if all([item_id, option_id, value_id, currency]):
            xml_payload = f'<response><option O="{option_id}" V="{value_id}"/></response>'
            encoded_xml = quote_plus(xml_payload)

            request_url = (
                "https://plati.market/asp/price_options.asp?"
                f"p={item_id}&"
                f"c={currency}&"
                f"x={encoded_xml}"
            )

            item = InsideProduct(
                price_text=price_text,
                request_url=request_url
            )

            options_data.append(item)
    return options_data


def _normalize_price_string(text: str) -> Optional[float]:
    if not text:
        return None
    try:
        price_match = re.search(r'[\d.,]+', text)
        if price_match:
            return float(price_match.group(0).replace(',', '.'))
    except (ValueError, AttributeError):
        return None
    return None
