import logging
import math
import random
import re  # <-- Thêm import re
from datetime import datetime
from typing import Dict, Any, List, Set, Optional

import httpx
# Bỏ import requests không dùng
# import requests

from clients.digiseller_client import DigisellerClient
from models.digiseller_models import BsProduct, ProductPriceUpdate, ProductPriceVariantUpdate
from models.sheet_models import Payload
from services.digiseller_service import get_product_list, analyze_product_offers, get_product_description


async def process_single_payload(payload: Payload, http_client: httpx.AsyncClient) -> Dict[str, Any]:
    logging.info(f"Processing payload for product: {payload.product_name} (Row: {payload.row_index})")
    product_update = None
    analysis_result = {}
    filtered_products = []  # Đổi tên: Đây là danh sách đã được lọc
    log_str = ""
    try:
        if not payload.is_compare_enabled:
            # Giả định: nếu không so sánh, giá min/max đã fetch LÀ GIÁ CUỐI CÙNG
            # (Nếu đây là lỗi, logic gốc của bạn cũng đã như vậy)
            if payload.fetched_min_price is None:
                raise ValueError("Không so sánh nhưng fetched_min_price là None")
            if payload.price_rounding is None:
                raise ValueError(f"Không có price_rounding (cột P) cho {payload.product_name}")
            final_price = round_up_to_n_decimals(payload.fetched_min_price, payload.price_rounding)
            if payload.fetched_max_price is not None:
                final_price = min(final_price, payload.fetched_max_price)

        else:
            compare_flow_result = await do_compare_flow(payload, http_client)
            final_price = compare_flow_result['final_price']
            analysis_result = compare_flow_result['analysis_result']
            filtered_products = compare_flow_result['filtered_products']  # Nhận danh sách đã lọc
            payload.target_price = analysis_result['competitive_price']

        if final_price is None and payload.is_compare_enabled:
            # Xảy ra khi không tìm thấy đối thủ VÀ không có max_price
            raise ValueError("Không tìm thấy giá đối thủ và fetched_max_price là None")
        elif final_price is None:
            raise ValueError("final_price là None (lỗi logic)")

        if not payload.is_have_min_price:
            log_str = get_log_string(mode="no_min_price", payload=payload, final_price=final_price,
                                     analysis_result=analysis_result, filtered_products=filtered_products)
        elif final_price < payload.get_min_price():
            log_str = get_log_string(mode="below_min", payload=payload, final_price=final_price,
                                     analysis_result=analysis_result, filtered_products=filtered_products)
        else:
            product_update = await prepare_price_update(final_price, payload)
            if payload.get_compare_type == 'noCompare':
                log_str = get_log_string(mode="not_compare", payload=payload, final_price=final_price)
            elif payload.get_compare_type == 'compare2' and product_update.is_ignore:
                log_str = get_log_string(
                    mode="compare2",
                    payload=payload,
                    final_price=payload.current_price,
                    analysis_result=analysis_result,
                    filtered_products=filtered_products
                )
            else:
                log_str = get_log_string(
                    mode="compare",
                    payload=payload,
                    final_price=final_price,
                    analysis_result=analysis_result,
                    filtered_products=filtered_products
                )
            # print(f"Prepared product update: {product_update.model_dump_json()}")
    except (ValueError, ConnectionError, TypeError) as e:  # Bắt thêm TypeError
        logging.error(f"Error processing {payload.product_name} (Row: {payload.row_index}): {e}")
        log_str = f"Lỗi: {e}"
    except Exception as e:
        # Bắt các lỗi khác
        logging.error(f"Unhandled Error processing {payload.product_name}: {e}", exc_info=True)
        log_str = f"Lỗi nghiêm trọng: {e}"

    current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_data = {
        'note': f"{log_str}",
        'last_update': current_time_str
    }
    return {
        'log_data': log_data,
        'product_update': product_update
    }


async def do_compare_flow(
        payload: Payload,
        http_client: httpx.AsyncClient
) -> Dict[str, Any]:
    if payload.product_compare is None:
        raise ValueError(f"Không có link so sánh (cột J) cho {payload.product_name}")

    try:
        response = await http_client.get(payload.product_compare)
        response.raise_for_status()
        html_str = response.text
    except httpx.RequestError as e:
        logging.error(f"HTTP error while fetching {payload.product_compare}: {e}")
        raise ConnectionError(f"Failed to fetch compare link: {e}") from e

    # --- THAY ĐỔI Ở ĐÂY ---
    # Truyền toàn bộ payload để get_product_list có thể lọc trước
    product_list = await get_product_list(html_str, payload)

    if not product_list:
        raise ValueError("No products found after fetching details")

    # BỎ filter_products (đã được thực hiện bên trong get_product_list)
    # filtered_product_list = filter_products(product_list, payload)

    if payload.fetched_min_price is None:
        raise ValueError(f"Không có fetched_min_price cho {payload.product_name}")
    if payload.fetched_black_list is None:
        payload.fetched_black_list = []  # Mặc định là list rỗng

    analysis_result = analyze_product_offers(
        offers=product_list,  # Sử dụng product_list đã được lọc sẵn
        min_price=payload.fetched_min_price,
        black_list=payload.fetched_black_list
    )

    if payload.price_rounding is None:
        raise ValueError(f"Không có price_rounding (cột P) cho {payload.product_name}")

    final_price = calc_final_price(
        price=analysis_result['competitive_price'],
        payload=payload
    )
    return {
        'final_price': final_price,
        'analysis_result': analysis_result,
        'filtered_products': product_list  # Trả về danh sách đã lọc
    }


def filter_products(products: List[BsProduct], payload: Payload) -> List[BsProduct]:
    # HÀM NÀY BÂY GIỜ KHÔNG CÒN ĐƯỢC SỬ DỤNG (logic đã chuyển vào get_product_list)
    # Chúng ta để lại nó để tránh lỗi nếu có nơi nào khác gọi
    logging.warning("filter_products() is deprecated and was called")
    filtered_products = []
    for product in products:
        if payload.include_keyword is not None:
            # Sửa: Lọc bỏ keyword rỗng
            include_kws = [kw.strip().lower() for kw in payload.include_keyword.split(',') if kw.strip()]
            if include_kws and not any(kw in product.name.lower() for kw in include_kws):
                continue
        if payload.exclude_keyword is not None:
            # Sửa: Lọc bỏ keyword rỗng
            exclude_kws = [kw.strip().lower() for kw in payload.exclude_keyword.split(',') if kw.strip()]
            if exclude_kws and any(kw in product.name.lower() for kw in exclude_kws):
                continue

        # Sửa: Chuyển sold_count sang int để so sánh
        try:
            sold = int(product.sold_count)
        except (ValueError, TypeError):
            sold = 0

        # Sửa: Chỉ lọc nếu order_sold > 0
        if payload.order_sold and payload.order_sold > 0 and sold < payload.order_sold:
            continue
        filtered_products.append(product)

    return filtered_products


async def prepare_price_update(price: float, payload: Payload) -> ProductPriceUpdate:
    """Creates a ProductPriceUpdate object without sending it."""

    if payload.product_id is None:
        raise ValueError(f"Không có product_id (cột G) cho {payload.product_name}")
    if payload.price_rounding is None:
        raise ValueError(f"Không có price_rounding (cột P) cho {payload.product_name}")

    # Tạo client mới cho hàm này vì nó là self-contained
    async with DigisellerClient() as client:
        result = await get_product_description(client=client, product_id=payload.product_id)

    if result is None:
        raise ValueError(f"Không tìm thấy thông tin sản phẩm Digiseller ID {payload.product_id}.")
    base_price = result.get('base_price')
    if base_price is None:
        raise ValueError(f"Không tìm thấy giá cơ bản của sản phẩm {payload.product_id}.")

    # Biến tạm để lưu variant_id tìm thấy từ API
    found_variant_api_id: Optional[int] = None

    if payload.product_variant_id is not None:
        variants = result.get('variants', [])  # Đây là list[dict]
        variant_found = False

        # --- BẮT ĐẦU SỬA LỖI LOGIC ---
        # Chuyển payload.product_variant_id (có thể là "10,20") thành list
        # và chỉ lấy keyword đầu tiên (vì chúng ta chỉ cập nhật 1 variant)
        keyword_str = str(payload.product_variant_id).split(',')[0].strip()

        if variants and keyword_str:
            # Tạo regex để tìm chính xác từ (ví dụ: '10' sẽ không khớp với '100')
            pattern = re.compile(r'\b' + re.escape(keyword_str) + r'\b', re.IGNORECASE)

            for vari in variants:
                variant_text = vari.get('text', '')

                # Logic tìm kiếm: Tìm keyword trong 'text' của variant
                if pattern.search(variant_text):
                    variant_found = True
                    _tmp_price = vari.get('modify_value', 0)
                    payload.current_price = base_price + _tmp_price

                    # Lấy 'value' (ID của SimpleVariant)
                    found_variant_api_id = vari.get('value', None)
                    break  # Thoát vòng lặp khi tìm thấy

        if not variant_found:
            logging.warning(
                f"Không tìm thấy variant khớp keyword '{keyword_str}' cho SP {payload.product_id} qua API. Sẽ thử dùng ID trực tiếp.")
            # Fallback: Giả định ID người dùng nhập (payload.product_variant_id) là ID ĐÚNG
            # Cố gắng chuyển nó thành int
            try:
                found_variant_api_id = int(keyword_str)
            except (ValueError, TypeError):
                raise ValueError(f"Keyword variant '{keyword_str}' không phải là số VÀ không tìm thấy trong text.")

        if found_variant_api_id is None:
            # Lỗi này xảy ra nếu keyword rỗng hoặc API trả về value=None
            raise ValueError(
                f"Không thể xác định variant_id cho SP {payload.product_id} và keyword {keyword_str}")
        # --- KẾT THÚC SỬA LỖI LOGIC ---

        _is_ignore = False
        if payload.current_price and payload.target_price and \
                payload.current_price < payload.target_price and \
                payload.get_compare_type == 'compare2':
            _is_ignore = True

        price_count = result.get('price_count', 1)
        if price_count is None or price_count == 0:
            price_count = 1
            logging.warning(f"price_count không hợp lệ cho SP {payload.product_id}, đặt lại là 1.")

        target_price_per_unit = price / price_count
        delta = target_price_per_unit - base_price
        _type = 'priceplus' if delta >= 0 else 'priceminus'

        variant = ProductPriceVariantUpdate(
            variant_id=found_variant_api_id,  # Dùng ID tìm thấy (chính là 'value')
            rate=abs(round_up_to_n_decimals(delta, payload.price_rounding)),
            type=_type,
            target_price=target_price_per_unit,
            price_rounding=payload.price_rounding,
        )

        return ProductPriceUpdate(
            product_id=payload.product_id,
            price=base_price,
            variants=[variant],
            is_ignore=_is_ignore,
        )
    else:
        # Trường hợp không dùng variant
        payload.current_price = base_price
        _is_ignore = False
        if payload.current_price and payload.target_price and \
                payload.current_price < payload.target_price and \
                payload.get_compare_type == 'compare2':
            _is_ignore = True

        return ProductPriceUpdate(
            product_id=payload.product_id,
            price=price,
            is_ignore=_is_ignore,
        )


def calc_final_price(price: Optional[float], payload: Payload) -> Optional[float]:
    # Sửa lỗi: Nếu không có đối thủ, price là None
    if price is None:
        # Nếu không có giá, dùng Max Price VÀ làm tròn lên ngay.
        if payload.fetched_max_price is None:
            logging.warning(f"Không có đối thủ và không có fetched_max_price cho {payload.product_name}")
            return None  # Không thể tính giá
        price = round_up_to_n_decimals(payload.fetched_max_price, payload.price_rounding)
        logging.info(f"No product match for {payload.product_name}, using fetched max price: {price:.3f}")

    # --- KHỐI ĐIỀU CHỈNH GIÁ (ADJUSTMENT) ---
    if payload.min_price_adjustment is not None and payload.max_price_adjustment is not None:
        min_adj = min(payload.min_price_adjustment, payload.max_price_adjustment)
        max_adj = max(payload.min_price_adjustment, payload.max_price_adjustment)
        d_price = random.uniform(min_adj, max_adj)
        price = price - d_price

    # --- KHỐI GIỚI HẠN (CAPPING) VÀ LÀM TRÒN ---

    # 1. Giới hạn dưới sau khi điều chỉnh
    if payload.fetched_min_price is not None:
        price = max(price, payload.fetched_min_price)

    # 2. Giới hạn trên sau khi điều chỉnh
    if payload.fetched_max_price is not None:
        price = min(price, payload.fetched_max_price)

    # 3. Làm tròn và đảm bảo không vượt Max
    if payload.price_rounding is not None:
        price = round_up_to_n_decimals(price, payload.price_rounding)
        # Quan trọng: GIỚI HẠN LẠI để đảm bảo giá làm tròn không vượt quá fetched_max_price
        if payload.fetched_max_price is not None:
            price = min(price, payload.fetched_max_price)

    return price


# Bỏ hàm calc_final_price_old không dùng
# def calc_final_price_old...

def round_up_to_n_decimals(number, n):
    if number is None:
        raise TypeError(f"round_up_to_n_decimals() received None instead of a number.")
    if n is None:
        logging.warning("price_rounding is None, defaulting to 0 decimal places.")
        n = 0
    if n < 0:
        raise ValueError("Number of decimal places (n) cannot be negative.")

    multiplier = 10 ** n
    return math.ceil(number * multiplier) / multiplier


def round_down_to_n_decimals(number, n):
    if number is None:
        raise TypeError(f"round_down_to_n_decimals() received None instead of a number.")
    if n is None:
        logging.warning("price_rounding is None, defaulting to 0 decimal places.")
        n = 0
    if n < 0:
        raise ValueError("Number of decimal places (n) cannot be negative.")

    multiplier = 10 ** n
    return math.floor(number * multiplier) / multiplier


def get_log_string(
        mode: str,
        payload: Payload,
        final_price: float,
        analysis_result: Dict[str, Any] = None,
        filtered_products: List[BsProduct] = None
) -> str:
    timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    log_parts = []

    # Sửa: Đảm bảo fetched_min_price và fetched_max_price không phải None trước khi format
    min_price_str = f"{payload.fetched_min_price:.3f}" if payload.fetched_min_price is not None else "N/A"
    max_price_str = f"{payload.fetched_max_price:.3f}" if payload.fetched_max_price is not None else "N/A"
    final_price_str = f"{final_price:.3f}" if final_price is not None else "N/A"

    if mode == "not_compare":
        log_parts = [
            timestamp,
            f" Không so sánh, cập nhật thành công {final_price_str}\n"
            f"Min price = {min_price_str}\nMax price = {max_price_str}\n"
        ]
    elif mode == "compare":
        log_parts = [
            timestamp,
            f" Cập nhật thành công {final_price_str}\n"
        ]
        if analysis_result:
            log_parts.append(_analysis_log_string(payload, analysis_result, filtered_products))
    elif mode == "compare2":
        current_price_str = f"{payload.current_price:.3f}" if payload.current_price is not None else "N/A"
        log_parts = [
            timestamp,
            f" Không cập nhật vì giá hiện tại ({current_price_str}) thấp hơn đối thủ:\n"
        ]
        if analysis_result:
            log_parts.append(_analysis_log_string(payload, analysis_result, filtered_products))
    elif mode == "below_min":
        min_price_val_str = f"{payload.get_min_price():.3f}" if payload.is_have_min_price else "N/A"
        log_parts = [
            timestamp,
            f" Giá cuối cùng ({final_price_str}) nhỏ hơn giá tối thiểu ({min_price_val_str}), không cập nhật.\n"
        ]
        if analysis_result:
            log_parts.append(_analysis_log_string(payload, analysis_result, filtered_products))
    elif mode == "no_min_price":
        log_parts = [
            timestamp,
            f" Không có min_price (cột AF) hoặc min_price = 0, không cập nhật.\n"
        ]
        if analysis_result:
            log_parts.append(_analysis_log_string(payload, analysis_result, filtered_products))
    return " ".join(log_parts)


def _analysis_log_string(
        payload: Payload,
        analysis_result: Dict[str, Any] = None,
        filtered_products: List[BsProduct] = None
) -> str:
    log_parts = []
    if analysis_result is None: analysis_result = {}
    if filtered_products is None: filtered_products = []

    competitor_price = analysis_result.get("competitive_price")

    if analysis_result.get("valid_competitor") is None:
        competitor_name = "Max price"
        # Nếu không có đối thủ, giá cạnh tranh là max_price
        if competitor_price is None:
            competitor_price = payload.fetched_max_price
    else:
        competitor_name = analysis_result.get("valid_competitor").seller_name

    price_str = f"{competitor_price:.6f}" if competitor_price is not None else "N/A"
    log_parts.append(f"- GiaSosanh: {competitor_name} = {price_str}\n")

    price_min_str = f"{payload.fetched_min_price:.6f}" if payload.fetched_min_price is not None else "None"
    price_max_str = f"{payload.fetched_max_price:.6f}" if payload.fetched_max_price is not None else "None"
    log_parts.append(f"PriceMin = {price_min_str}, PriceMax = {price_max_str}\n")

    sellers_below = analysis_result.get("sellers_below_min", [])
    if sellers_below:
        # Lọc ra các seller không có trong blacklist
        safe_blacklist = {n.lower() for n in payload.fetched_black_list} if payload.fetched_black_list else set()
        valid_sellers_below = [s for s in sellers_below if
                               s.seller_name and s.seller_name.lower() not in safe_blacklist]
        if valid_sellers_below:
            sellers_info = "; ".join([f"{s.seller_name} = {s.get_price():.6f}\n" for s in valid_sellers_below[:6]])
            log_parts.append(f"Seller giá nhỏ hơn min_price (không blacklist):\n {sellers_info}")

    log_parts.append("Top 4 sản phẩm (đã lọc):\n")
    # Sửa: filtered_products đã được sắp xếp trong analyze_product_offers
    # sorted_product = sorted(filtered_products, key=lambda item: item.get_price(), reverse=False)
    for product in filtered_products[:4]:
        price_val = product.get_price()
        price_str = f"{price_val:.6f}" if price_val is not None else "N/A"
        log_parts.append(f"- {product.name} ({product.seller_name}): {price_str}\n")

    return "".join(log_parts)


def consolidate_price_updates(updates: List[ProductPriceUpdate]) -> List[ProductPriceUpdate]:
    """
    Gộp nhiều bản cập nhật, ưu tiên giá từ các bản cập nhật giá cơ bản thuần túY.
    """
    # Loại bỏ những ProductPriceUpdate.is_ignore = True
    updates = [update for update in updates if not (update and update.is_ignore)]
    consolidated: Dict[int, ProductPriceUpdate] = {}
    has_authoritative_base_price: Set[int] = set()

    for update in updates:
        if not update:
            continue

        pid = update.product_id
        is_pure_base_update = update.variants is None and update.price is not None

        if pid not in consolidated:
            consolidated[pid] = update.model_copy(deep=True)
            if is_pure_base_update:
                has_authoritative_base_price.add(pid)
            continue

        existing_update = consolidated[pid]

        if is_pure_base_update:
            existing_update.price = update.price
            has_authoritative_base_price.add(pid)
        elif update.price is not None and pid not in has_authoritative_base_price:
            # Chỉ cập nhật giá base nếu nó chưa được set bởi một pure_base_update
            existing_update.price = update.price

        if update.variants:
            if existing_update.variants is None:
                existing_update.variants = []

            # Ghi đè variant nếu đã tồn tại
            existing_variants_map = {v.variant_id: v for v in existing_update.variants}
            for new_variant in update.variants:
                if new_variant.variant_id in existing_variants_map:
                    # Ghi đè variant cũ
                    existing_update.variants.remove(existing_variants_map[new_variant.variant_id])
                existing_update.variants.append(new_variant)

    final_updates: List[ProductPriceUpdate] = []
    for pid, final_update in consolidated.items():
        definitive_price = final_update.price
        if final_update.variants and definitive_price is not None:
            # Tính toán lại delta dựa trên giá base cuối cùng
            for variant in final_update.variants:
                if variant.target_price is None:
                    logging.warning(f"Variant {variant.variant_id} cho SP {pid} thiếu target_price, bỏ qua.")
                    continue

                final_delta = variant.target_price - definitive_price
                rounding = variant.price_rounding if variant.price_rounding is not None else 0
                variant.rate = abs(round_up_to_n_decimals(final_delta, rounding))
                variant.type = 'priceplus' if final_delta >= 0 else 'priceminus'

        final_updates.append(final_update)

    return final_updates

