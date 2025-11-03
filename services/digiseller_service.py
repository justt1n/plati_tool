import asyncio
import logging
import re
from typing import List, Optional, Dict, Any
from urllib.parse import quote_plus

import httpx
# Thêm imports cho Tenacity
import tenacity
from tenacity import AsyncRetrying, wait_exponential, stop_after_attempt, retry_if_exception, RetryError
from bs4 import BeautifulSoup

from clients.digiseller_client import DigisellerClient
from clients.google_sheets_client import GoogleSheetsClient
from models.digiseller_models import SellerItem, BsProduct, InsideProduct, InsideInfo
# Thêm import Payload để type hint
from models.sheet_models import Payload
from utils.config import settings

logger = logging.getLogger(__name__)

HEADER = [
    "ID", "Product Name", "Price", "Currency", "Price (USD)", "Price (RUR)", "Price (EUR)", "Price (UAH)",
    "In Stock", "Items in Stock", "Sales", "Returns", "Good Responses", "Bad Responses",
    "Sales (Hidden)", "Returns (Hidden)", "Good Responses (Hidden)", "Bad Responses (Hidden)",
    "Agent Commission", "Visible", "Has Discount", "Num Options", "Owner ID",
    "Release Date", "Info", "Additional Info", "Sale Info (JSON)"
]


# --- HÀM HELPER MỚI CHO TENACITY ---
def is_retryable_http_error(exception: BaseException) -> bool:
    """Chỉ retry nếu là ConnectionError hoặc lỗi HTTP 5xx/429."""
    if isinstance(exception, ConnectionError):
        return True
    if isinstance(exception, httpx.RequestError):
        # Retry on network errors, timeout
        return True

    if isinstance(exception, httpx.HTTPStatusError):
        # Chỉ retry nếu server bị lỗi (5xx) hoặc bị rate limit (429)
        # Sẽ KHÔNG retry nếu là 404 (Not Found) or 403 (Forbidden)
        is_server_error = exception.response.status_code >= 500
        is_rate_limit = exception.response.status_code == 429
        return is_server_error or is_rate_limit

    # Không retry các lỗi khác (ví dụ: lỗi logic, Pydantic)
    return False


# --- KẾT THÚC HÀM HELPER ---


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

            response = await client.get_seller_items(page=current_page, rows=500)

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


def analyze_product_offers(
        offers: List[BsProduct],
        min_price: float,  # Sửa: min_price không nên là Optional ở đây
        black_list: Optional[List[str]] = None,
) -> Dict[str, Any]:
    if min_price is None:
        # Đây là một lỗi logic nếu xảy ra
        logging.error("analyze_product_offers được gọi với min_price là None.")
        min_price = 0  # Đặt giá trị mặc định để tránh crash

    if not offers:
        return {
            "valid_competitor": None,
            "competitive_price": None,
            "top_sellers_for_log": [],
            "sellers_below_min": []
        }

    safe_blacklist = {name.lower() for name in (black_list or [])}

    # Sắp xếp các offer HỢP LỆ (có giá và không nằm trong blacklist)
    valid_offers = []
    for o in offers:
        price = o.get_price()
        # Sửa: Phải kiểm tra seller_name trước khi dùng lower()
        if price is not None and o.seller_name and o.seller_name.lower() not in safe_blacklist:
            valid_offers.append(o)

    sorted_offers = sorted(valid_offers, key=lambda o: o.get_price())

    valid_competitor = None
    competitive_price = None

    # Tìm đối thủ cạnh tranh đầu tiên có giá >= min_price
    for offer in sorted_offers:
        offer_price = offer.get_price()  # Đã kiểm tra not None ở trên
        if offer_price >= min_price:
            valid_competitor = offer
            competitive_price = offer_price
            break  # Dừng ngay khi tìm thấy đối thủ hợp lệ đầu tiên

    sellers_below_min = []

    # Lấy TẤT CẢ offer (bao gồm cả blacklist) để log
    all_sorted_offers = sorted(offers, key=lambda o: o.get_price() or float('inf'))

    # Nếu tìm thấy đối thủ, log tất cả những ai rẻ hơn đối thủ
    if competitive_price is not None:
        for offer in all_sorted_offers:
            offer_price = offer.get_price()
            if offer_price is not None and offer_price < competitive_price:
                sellers_below_min.append(offer)
    # Nếu không tìm thấy đối thủ, log tất cả những ai rẻ hơn min_price
    else:
        for offer in all_sorted_offers:
            offer_price = offer.get_price()
            if offer_price is not None and offer_price < min_price:
                sellers_below_min.append(offer)

    analysis = {
        "valid_competitor": valid_competitor,
        "competitive_price": competitive_price,
        "top_sellers_for_log": all_sorted_offers[:4],  # Log top 4 BẤT KỂ blacklist
        "sellers_below_min": sellers_below_min  # Log seller dưới min (để kiểm tra)
    }

    return analysis


# --- HÀM ĐƯỢC TỐI ƯU HÓA CAO ---
async def get_product_list(html_str: str, payload: Payload) -> List[BsProduct]:
    if not html_str:
        logger.error("No HTML content provided to get product list.")
        return []

    soup = BeautifulSoup(html_str, 'html.parser')
    product_list_container = soup.find('ul', id='item_list')

    if not product_list_container:
        product_list_container = soup.find('ul', id='itemsList')
        if not product_list_container:
            logger.warning("Could not find the product list container (item_list or itemsList)")
            return []

    product_cards = product_list_container.find_all('li', class_='section-list__item')

    # === BƯỚC 1: Parse thông tin CÓ SẴN từ trang tìm kiếm ===
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
                                                                                ' ').strip() if price_tag else None  # Sửa: là None nếu không thấy

        sold_tag = product_link_tag.find('span', string=lambda text: text and 'Sold' in text)
        sold_count = sold_tag.get_text(strip=True).replace('Sold',
                                                           '').strip() if sold_tag else "0"  # Sửa: là "0" nếu không thấy

        img_tag = product_link_tag.find('img', class_='preview-image')
        img_url = img_tag.get('src', 'N/A') if img_tag else 'N/A'
        if img_url.startswith('//'):
            img_url = 'https:' + img_url

        product_instance = BsProduct(
            name=name,
            outside_price=outside_price,
            sold_count=sold_count,  # Dùng sold_count từ trang tìm kiếm
            link=full_link,
            image_link=img_url,
            price=None  # Giá variant sẽ được lấy sau
        )
        initial_products.append(product_instance)

    if not initial_products:
        logger.warning("Trang tìm kiếm không có sản phẩm nào.")
        return []

    # === BƯỚC 2: LỌC TRƯỚC (PRUNING) dựa trên thông tin vừa parse ===
    pre_filtered_products = []

    # Chuẩn bị keyword 1 lần VÀ lọc bỏ các chuỗi rỗng
    include_kws = [kw.strip().lower() for kw in payload.include_keyword.split(',') if
                   kw.strip()] if payload.include_keyword else []
    exclude_kws = [kw.strip().lower() for kw in payload.exclude_keyword.split(',') if
                   kw.strip()] if payload.exclude_keyword else []

    for product in initial_products:
        product_name_lower = product.name.lower()

        # Logic này (any) có nghĩa là "chỉ cần khớp 1 keyword là được"
        if include_kws:
            if not any(kw in product_name_lower for kw in include_kws):
                continue  # Bỏ qua nếu không chứa BẤT KỲ keyword BẮT BUỘC nào

        # Logic này (any) có nghĩa là "nếu khớp 1 keyword là bỏ"
        if exclude_kws:
            if any(kw in product_name_lower for kw in exclude_kws):
                continue  # Bỏ qua nếu chứa BẤT KỲ keyword CẤM nào

        try:
            sold = int(product.sold_count)
        except (ValueError, TypeError):
            sold = 0

        # Chỉ lọc nếu payload.order_sold được set (không phải None và > 0)
        if payload.order_sold and payload.order_sold > 0:
            if sold < payload.order_sold:
                continue  # Bỏ qua nếu không đủ số lượng bán

        pre_filtered_products.append(product)

    # logging.info(f"Lọc trước (Pruning): từ {len(initial_products)} -> {len(pre_filtered_products)} sản phẩm.")

    # === THAY ĐỔI: BƯỚC 2.5 Giới hạn Top 5 (Theo yêu cầu) ===
    # Giới hạn số lượng sản phẩm cần fetch chi tiết để tránh bị ban
    limit = settings.LIMIT_PROD
    if len(pre_filtered_products) > limit:
        # logging.info(
        #     f"Đã cắt bớt danh sách từ {len(pre_filtered_products)} -> {limit} sản phẩm (Top 5 giá rẻ nhất đã lọc).")
        pre_filtered_products = pre_filtered_products[:limit]
    # === KẾT THÚC THAY ĐỔI ===

    if not pre_filtered_products:
        return []

    # === BƯỚC 3: Chỉ fetch thông tin chi tiết cho các sản phẩm ĐÃ LỌC (và giới hạn) ===
    async with httpx.AsyncClient(timeout=10.0) as client:
        price_tasks = [
            # Truyền key_words từ payload.product_compare2
            _get_inside_info(p.link, payload.product_compare2, client) for p in pre_filtered_products
        ]
        infos = await asyncio.gather(*price_tasks, return_exceptions=True)

    # === BƯỚC 4: Gán thông tin chi tiết (seller và giá variant) ===
    for product, info in zip(pre_filtered_products, infos):
        if isinstance(info, InsideInfo):
            product.price = info.price  # Gán giá variant (có thể là None hoặc -1.0)
            product.seller_name = info.seller_name
            # KHÔNG ghi đè sold_count
            # product.sold_count = info.order_sold_count
        elif isinstance(info, Exception):
            # Lỗi này là lỗi Pydantic mà bạn thấy
            logger.error(f"Failed to fetch price for {product.name} (Exception in gather): {info}")
            product.price = -1.0  # Gán -1.0 nếu có lỗi
            product.seller_name = "Unknown (Gather Error)"

    return pre_filtered_products


async def _get_inside_info(link: str, key_words: Optional[str], client: httpx.AsyncClient) -> InsideInfo:
    # Định nghĩa chiến lược retry
    # Bắt đầu chờ 1s, rồi 2s, 4s (tối đa 5s). Tổng cộng 4 lần.
    retryer = AsyncRetrying(
        wait=wait_exponential(multiplier=1, min=1, max=5),
        stop=stop_after_attempt(4),
        retry=retry_if_exception(is_retryable_http_error),
        reraise=True  # Ném ra lỗi gốc sau khi retry thất bại
    )

    html_content = ""
    try:
        link += "?lang=en"

        # Bọc request 1 (lấy trang HTML) trong retryer
        async def fetch_page():
            response = await client.get(link)
            response.raise_for_status()
            return response.text

        html_content = await retryer(fetch_page)

        seller_name = _get_seller_info(html_content)
        sold_count = _get_order_sold_count(html_content)  # Vẫn lấy, nhưng sẽ không dùng
        price = None  # Bắt đầu là None

        if key_words:  # Chỉ lấy giá variant nếu có key_words
            options = _extract_price_options_with_url(html_content, currency=settings.CURRENCY)
            url_price = _find_option_url_by_keywords(options, key_words)

            if url_price:
                # Bọc request 2 (lấy giá JSON) trong retryer
                async def fetch_price():
                    price_response = await client.get(url_price)
                    price_response.raise_for_status()
                    return price_response.json()

                price_data = await retryer(fetch_price)

                try:
                    price_value = price_data.get('amount')
                    count = price_data.get('count', 1)
                    if price_value is not None:
                        price = float(price_value.replace(',', '.')) / count
                except (ValueError, TypeError):
                    price = None  # Lỗi parse JSON -> giá là None
            else:
                # Có key_words nhưng không tìm thấy URL
                # logging.warning(f"Không tìm thấy variant cho keywords '{key_words}' tại link {link}")
                price = None

                # Chuyển None thành -1.0 để Pydantic (float) chấp nhận
        final_price = price if price is not None else -1.0

        info = InsideInfo(
            seller_name=seller_name,
            price=final_price,  # Sẽ là float (-1.0 nếu None), không bao giờ None
            order_sold_count=sold_count
        )
        return info

    except RetryError as e:
        # Lỗi SAU KHI đã retry hết 4 lần
        logger.error(f"Error fetching inside page from {link} (FAILED after 4 retries): {e.last_attempt.exception()}")
        return InsideInfo(
            seller_name="Unknown (Retry Failed)",
            price=-1.0,
            order_sold_count=0
        )
    except Exception as e:
        # Bắt các lỗi không thể retry (ví dụ: lỗi parse HTML/BeautifulSoup, hoặc 404)
        logger.error(f"Error processing inside page from {link} (Non-retryable): {e}")
        return InsideInfo(
            seller_name="Unknown (Parse Error)",
            price=-1.0,
            order_sold_count=0
        )


def _find_option_url_by_keywords(options: List[InsideProduct], keywords_str: str) -> Optional[str]:
    keywords = [k.strip() for k in keywords_str.split(",") if k.strip()]
    for keyword in keywords:
        target_price = _normalize_price_string(keyword)

        # 1. Thử tìm bằng giá (nếu keyword là số)
        if target_price is not None:
            for option in options:
                option_price = _normalize_price_string(option.price_text)
                if option_price is not None and abs(target_price - option_price) < 1e-9:
                    return option.request_url

        # 2. Thử tìm bằng text (nếu keyword là chữ)
        pattern = re.compile(r'\b' + re.escape(keyword) + r'\b', re.IGNORECASE)
        for option in options:
            if pattern.search(option.price_text):
                return option.request_url
    return None


def _extract_price_options_with_url(html_str: str, currency: str = 'RUB') -> List[InsideProduct]:
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

        if all([item_id, option_id, value_id, currency]):
            xml_payload = f'<response><option O="{option_id}" V="{value_id}"/></response>'
            encoded_xml = quote_plus(xml_payload)

            request_url = (
                "https://plati.market/asp/price_options.asp?"
                f"p={item_id}&"
                f"n=1&"
                f"c={currency}&"
                f"e=&"
                f"d=false&"
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
        # Tìm số float (có thể có , hoặc .)
        price_match = re.search(r'[\d.,]+', text)
        if price_match:
            return float(price_match.group(0).replace(',', '.'))
    except (ValueError, AttributeError):
        return None
    return None


def _get_seller_info(html_str: str) -> str:
    if not html_str:
        return ""

    soup = BeautifulSoup(html_str, 'html.parser')

    # Tìm seller name
    seller_link_tag = soup.select_one("a[id$='seller_info_btn1']")

    if seller_link_tag:
        seller_name_tag = seller_link_tag.find('span', class_='body-semibold')
        if seller_name_tag:
            return seller_name_tag.get_text(strip=True)

    # Fallback: Thử tìm trong seller-info (cho layout khác)
    seller_info_div = soup.find('div', class_='seller-info__name')
    if seller_info_div:
        return seller_info_div.get_text(strip=True)

    return "Cant get seller name"


def _get_order_sold_count(html_str: str) -> int:
    if not html_str:
        return 0

    soup = BeautifulSoup(html_str, 'html.parser')
    # Tìm thẻ span có text 'Продано' (Tiếng Nga) hoặc 'Sold' (Tiếng Anh)
    sold_tag = soup.find('span', string=re.compile(r'(Продано|Sold)', re.IGNORECASE))

    if sold_tag:
        sold_text = sold_tag.get_text(strip=True)
        # Lấy tất cả các số trong text đó (ví dụ: "Sold: 1234")
        match = re.search(r'\d+', sold_text)
        if match:
            try:
                return int(match.group(0))
            except ValueError:
                return 0

    return 0


async def get_product_description(client: DigisellerClient, product_id: int) -> Optional[
    Dict[str, Any]]:
    try:
        res = await client.get_product_description(product_id)
        if not res or not res.product:
            logger.warning(f"Product with ID {product_id} not found.")
            return None

        product = res.product

        # Tìm variants
        variants = []
        if product.options:
            for opt in product.options:
                if opt.type in ['radio', 'select'] and opt.variants:
                    variants = opt.variants  # Lấy list variants đầu tiên tìm thấy
                    break

        count = 1
        base_price = None

        # Ưu tiên lấy giá RUB
        if product.prices and product.prices.initial and product.prices.initial.rub:
            base_price = product.prices.initial.rub
        # Fallback lấy giá theo đơn vị (nếu có)
        elif product.units and product.units.get('price') is not None:
            base_price = product.units.get('price')
            if product.prices_unit and product.prices_unit.unit_cnt:
                count = product.prices_unit.unit_cnt
        else:
            logger.warning(f"Không thể xác định giá cơ bản (base_price) cho SP {product_id}")
            return None  # Không có giá thì không thể xử lý

        return {
            'base_price': base_price,
            'variants': [v.model_dump() for v in variants],  # Trả về list[dict]
            'price_count': count,
        }
    except Exception as e:
        logger.error(f"Error fetching product description for ID {product_id}: {e}", exc_info=True)
        return None

