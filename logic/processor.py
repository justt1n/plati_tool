# logic/processor.py
import logging
import math
import random
from datetime import datetime
from typing import Dict, Any, List, Set

import httpx
import requests

from clients.digiseller_client import DigisellerClient
from models.digiseller_models import BsProduct, ProductPriceUpdate, ProductPriceVariantUpdate
from models.sheet_models import Payload
from services.digiseller_service import get_product_list, analyze_product_offers, get_product_description


async def process_single_payload(payload: Payload, http_client: httpx.AsyncClient) -> Dict[str, Any]:
    logging.info(f"Processing payload for product: {payload.product_name} (Row: {payload.row_index})")
    product_update = None
    analysis_result = {}
    filtered_products = []
    log_str = ""
    try:
        if not payload.is_compare_enabled:
            final_price = round_up_to_n_decimals(payload.fetched_min_price, payload.price_rounding)

        else:
            compare_flow_result = await do_compare_flow(payload, http_client)
            final_price = compare_flow_result['final_price']
            analysis_result = compare_flow_result['analysis_result']
            filtered_products = compare_flow_result['filtered_products']
            payload.target_price = analysis_result['competitive_price']
        if final_price is not None:
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
    except (ValueError, ConnectionError) as e:
        logging.error(f"Error processing {payload.product_name}: {e}")
        log_str = f"Lỗi: {e}"

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
    try:
        response = await http_client.get(payload.product_compare)
        response.raise_for_status()
        html_str = response.text
    except httpx.RequestError as e:
        logging.error(f"HTTP error while fetching {payload.product_compare}: {e}")
        raise ConnectionError(f"Failed to fetch compare link: {e}") from e

    product_list = await get_product_list(html_str, payload.product_compare2)
    if not product_list:
        raise ValueError("No products found in the provided link")
    filtered_product_list = filter_products(product_list, payload)

    analysis_result = analyze_product_offers(
        offers=filtered_product_list,
        min_price=payload.fetched_min_price,
        black_list=payload.fetched_black_list
    )
    final_price = calc_final_price(
        price=analysis_result['competitive_price'],
        payload=payload
    )
    return {
        'final_price': final_price,
        'analysis_result': analysis_result,
        'filtered_products': filtered_product_list
    }


def filter_products(products: List[BsProduct], payload: Payload) -> List[BsProduct]:
    filtered_products = []
    for product in products:
        if payload.include_keyword is not None:
            include_kws = payload.include_keyword.split(',')
            if not any(kw.strip().lower() in product.name.lower() for kw in include_kws):
                continue
        if payload.exclude_keyword is not None:
            exclude_kws = payload.exclude_keyword.split(',')
            if any(kw.strip().lower() in product.name.lower() for kw in exclude_kws):
                continue
        if int(product.sold_count) < payload.order_sold:
            continue
        filtered_products.append(product)

    return filtered_products


async def prepare_price_update(price: float, payload: Payload) -> ProductPriceUpdate:
    """Creates a ProductPriceUpdate object without sending it."""
    result = await get_product_description(client=DigisellerClient(), product_id=payload.product_id)
    if result is None:
        raise ValueError("Không tìm thấy thông tin sản phẩm.")
    base_price = result.get('base_price')
    if base_price is None:
        raise ValueError("Không tìm thấy giá cơ bản của sản phẩm.")
    if payload.product_variant_id is not None:
        variants = result.get('product', [])
        for item in variants:
            varilist = item.get('variants', [])
            for vari in varilist:
                if vari.get('value', '') == payload.product_variant_id:
                    _tmp_default = vari.get('default', 1)
                    if _tmp_default != 1:
                        _tmp_price = vari.get('modify_value', 0)
                        payload.current_price = base_price + _tmp_price
                    payload.product_variant_id = vari.get('id', None)
        _is_ignore = False
        if payload.current_price < payload.target_price and payload.get_compare_type == 'compare2':
            _is_ignore = True

        price_count = result.get('price_count', 1)
        if price_count is None:
            raise ValueError("Không tìm thấy đơn vị giá của sản phẩm.")

        target_price_per_unit = price / price_count
        delta = target_price_per_unit - base_price
        _type = 'priceplus' if delta >= 0 else 'priceminus'

        variant = ProductPriceVariantUpdate(
            variant_id=payload.product_variant_id,
            rate=abs(round_up_to_n_decimals(delta, payload.price_rounding)),
            type=_type,
            target_price=target_price_per_unit,
            # THAY ĐỔI: Lưu lại thông tin làm tròn
            price_rounding=payload.price_rounding,
        )

        return ProductPriceUpdate(
            product_id=payload.product_id,
            price=base_price,
            variants=[variant],
            is_ignore=_is_ignore,
        )
    else:
        payload.current_price = base_price
        _is_ignore = False
        if payload.current_price < payload.target_price and payload.get_compare_type == 'compare2':
            _is_ignore = True
        return ProductPriceUpdate(
            product_id=payload.product_id,
            price=price,
            is_ignore=_is_ignore,
        )


def calc_final_price(price: float, payload: Payload) -> float:
    if price is None:
        # Nếu không có giá, dùng Max Price VÀ làm tròn lên ngay.
        price = round_up_to_n_decimals(payload.fetched_max_price, payload.price_rounding)
        logging.info(f"No product match, using fetched max price: {price:.3f}")

    # --- KHỐI ĐIỀU CHỈNH GIÁ (ADJUSTMENT) ---
    if payload.min_price_adjustment is None or payload.max_price_adjustment is None:
        pass
    else:
        min_adj = min(payload.min_price_adjustment, payload.max_price_adjustment)
        max_adj = max(payload.min_price_adjustment, payload.max_price_adjustment)

        d_price = random.uniform(min_adj, max_adj)

        # Áp dụng mức giảm
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
        # Làm tròn lên để đạt giá trị cao nhất có thể (ví dụ: lên 513.000)
        price = round_up_to_n_decimals(price, payload.price_rounding)

        # Quan trọng: GIỚI HẠN LẠI để đảm bảo giá làm tròn không vượt quá fetched_max_price
        if payload.fetched_max_price is not None:
            price = min(price, payload.fetched_max_price)

    return price


def calc_final_price_old(price: float, payload: Payload) -> float:
    if price is None:
        price = round_up_to_n_decimals(payload.fetched_max_price, payload.price_rounding)
        logging.info(f"No product match, using fetched max price: {price:.3f}")
    if payload.min_price_adjustment is None or payload.max_price_adjustment is None:
        pass
    else:
        min_adj = min(payload.min_price_adjustment, payload.max_price_adjustment)
        max_adj = max(payload.min_price_adjustment, payload.max_price_adjustment)

        d_price = random.uniform(min_adj, max_adj)
        price = price - d_price

    if payload.fetched_min_price is not None:
        price = max(price, payload.fetched_min_price)

    if payload.fetched_max_price is not None:
        price = min(price, payload.fetched_max_price)

    if payload.price_rounding is not None:
        new_price = round_up_to_n_decimals(price, payload.price_rounding)
        if new_price > payload.fetched_max_price:
            price = round_down_to_n_decimals(payload.fetched_max_price, payload.price_rounding)
        else:
            price = new_price

    return price


def round_up_to_n_decimals(number, n):
    if n < 0:
        raise ValueError("Number of decimal places (n) cannot be negative.")

    multiplier = 10 ** n
    return math.ceil(number * multiplier) / multiplier


def round_down_to_n_decimals(number, n):
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
    if mode == "not_compare":
        log_parts = [
            timestamp,
            f"Không so sánh, cập nhật thành công {final_price:.3f}\nMin price = {payload.fetched_min_price:.3f}\nMax price = {payload.fetched_max_price:.3f}\n"
        ]
    elif mode == "compare":
        log_parts = [
            timestamp,
            f"Cập nhật thành công {final_price:.3f}\n"
        ]
        if analysis_result:
            log_parts.append(_analysis_log_string(payload, analysis_result, filtered_products))
    elif mode == "compare2":
        log_parts = [
            timestamp,
            f"Không cập nhật vì giá hiện tại thấp hơn đối thủ:\nGiá hiện tại: {payload.current_price:.3f}\n"
        ]
        if analysis_result:
            log_parts.append(_analysis_log_string(payload, analysis_result, filtered_products))
    elif mode == "below_min":
        log_parts = [
            timestamp,
            f"Giá cuối cùng ({final_price:.3f}) nhỏ hơn giá tối thiểu ({payload.get_min_price():.3f}), không cập nhật.\n"
        ]
        if analysis_result:
            log_parts.append(_analysis_log_string(payload, analysis_result, filtered_products))
    elif mode == "no_min_price":
        log_parts = [
            timestamp,
            f"Không có min_price, không cập nhật.\n"
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
    if analysis_result.get("valid_competitor") is None:
        competitor_name = "Max price"
    else:
        competitor_name = analysis_result.get("valid_competitor").seller_name
    competitor_price = analysis_result.get("competitive_price")
    if competitor_price is None:
        competitor_price = payload.fetched_max_price
    if competitor_name and competitor_price is not None:
        log_parts.append(f"- GiaSosanh: {competitor_name} = {competitor_price:.6f}\n")

    price_min_str = f"{payload.fetched_min_price:.6f}" if payload.fetched_min_price is not None else "None"
    price_max_str = f"{payload.fetched_max_price:.6f}" if payload.fetched_max_price is not None else "None"
    log_parts.append(f"PriceMin = {price_min_str}, PriceMax = {price_max_str}\n")

    sellers_below = analysis_result.get("sellers_below_min", [])
    if sellers_below:
        sellers_info = "; ".join([f"{s.seller_name} = {s.get_price():.6f}\n" for s in sellers_below[:6] if
                                  s.seller_name not in payload.fetched_black_list])
        log_parts.append(f"Seller giá nhỏ hơn min_price):\n {sellers_info}")

    log_parts.append("Top 4 sản phẩm:\n")
    sorted_product = sorted(filtered_products, key=lambda item: item.get_price(), reverse=False)
    for product in sorted_product[:4]:
        log_parts.append(f"- {product.name} ({product.seller_name}): {product.get_price():.6f}\n")

    return "".join(log_parts)


def consolidate_price_updates(updates: List[ProductPriceUpdate]) -> List[ProductPriceUpdate]:
    """
    Gộp nhiều bản cập nhật, ưu tiên giá từ các bản cập nhật giá cơ bản thuần túy.
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
            existing_update.price = update.price

        if update.variants:
            if existing_update.variants is None:
                existing_update.variants = []
            existing_update.variants.extend(update.variants)

    final_updates: List[ProductPriceUpdate] = []
    for pid, final_update in consolidated.items():
        definitive_price = final_update.price
        if final_update.variants and definitive_price is not None:
            for variant in final_update.variants:
                final_delta = variant.target_price - definitive_price
                variant.rate = abs(round_up_to_n_decimals(final_delta, variant.price_rounding))
                variant.type = 'priceplus' if final_delta >= 0 else 'priceminus'
        final_updates.append(final_update)

    return final_updates
