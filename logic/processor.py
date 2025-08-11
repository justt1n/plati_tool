# logic/processor.py
import logging
import random
from datetime import datetime
from typing import Dict, Any, List

import httpx
import requests

from clients.digiseller_client import DigisellerClient
from models.digiseller_models import BsProduct, ProductPriceUpdate, ProductPriceVariantUpdate
from models.sheet_models import Payload
from services.digiseller_service import get_product_list, analyze_product_offers


async def process_single_payload(client: DigisellerClient, payload: Payload) -> Dict[str, Any]:
    logging.info(f"Processing payload for product: {payload.product_name} (Row: {payload.row_index})")
    try:
        if not payload.is_compare_enabled:
            final_price = payload.fetched_min_price
            log_str = get_log_string(mode="not_compare", payload=payload, final_price=final_price)

        else:
            compare_flow_result = await do_compare_flow(payload)
            final_price = compare_flow_result['final_price']
            analysis_result = compare_flow_result['analysis_result']
            filtered_products = compare_flow_result['filtered_products']
            log_str = get_log_string(
                mode="compare",
                payload=payload,
                final_price=final_price,
                analysis_result=analysis_result,
                filtered_products=filtered_products
            )
        await edit_product_price(client, final_price, payload)
    except (ValueError, ConnectionError) as e:
        logging.error(f"Error processing {payload.product_name}: {e}")
        log_str = f"Lỗi: {e}"

    current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    result = {
        'note': f"{log_str}",
        'last_update': current_time_str
    }
    return result


async def do_compare_flow(payload: Payload) -> Dict[str, Any]:
    html_str = requests.get(payload.product_compare).text
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
        if payload.include_keyword is not None and payload.include_keyword not in product.name:
            continue
        if payload.exclude_keyword is not None and payload.exclude_keyword in product.name:
            continue
        if int(product.sold_count) < payload.order_sold:
            continue
        filtered_products.append(product)

    return filtered_products


async def edit_product_price(
    client: DigisellerClient,
    price: float, payload: Payload,
    have_variant: bool = False) -> bool:
    if have_variant:
        variant = ProductPriceVariantUpdate(variant_id=000, rate=2.0, type='priceplus')
        product_update = ProductPriceUpdate(
            product_id=payload.product_id,
            variants=[variant],
            price=price,
        )
    else:
        product_update = ProductPriceUpdate(
            product_id=payload.product_id,
            price=price,
        )
    try:
        response = await client.bulk_update_prices([product_update])
        if response.taskId is not None:
            logging.info(f"Price updated successfully for {payload.product_name} (Row: {payload.row_index})")
            return True
        else:
            logging.error(f"Failed to update price for {payload.product_name} (Row: {payload.row_index})")
            raise ValueError(f"Cannot update price for {payload.product_name}: No task ID returned")
    except (httpx.HTTPStatusError, httpx.RequestError) as e:
        logging.error(f"Error updating price for {payload.product_name} (Row: {payload.row_index}): {e}")
        raise ValueError(f"Cannot update price for {payload.product_name}: {e}")


def calc_final_price(price: float, payload: Payload) -> float:
    if payload.min_price_adjustment is None or payload.max_price_adjustment is None:
        pass
    else:
        min_adj = min(payload.min_price_adjustment, payload.max_price_adjustment)
        max_adj = max(payload.min_price_adjustment, payload.max_price_adjustment)

        d_price = random.uniform(min_adj, max_adj)
        price = price - d_price

    if payload.price_rounding is not None:
        price = round(price, payload.price_rounding)

    if payload.fetched_min_price is not None:
        price = max(price, payload.fetched_min_price)

    if payload.fetched_max_price is not None:
        price = min(price, payload.fetched_max_price)

    return price


def get_log_string(
    mode: str,
    payload: Payload,
    final_price: float,
    analysis_result: Dict[str, Any] = None,
    filtered_products: List[BsProduct] = None
) -> str:
    timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    if mode == "not_compare":
        return (
            f"{timestamp} {payload.product_name} (Row {payload.row_index}): "
            f"Không so sánh giá. Áp dụng giá min price: {final_price:.3f}"
        )

    elif mode == "compare":
        log_parts = [
            timestamp,
            f"Cập nhật thành công {final_price:.3f}\n"
        ]

        if analysis_result:
            competitor_name = analysis_result.get("valid_competitor").seller_name
            competitor_price = analysis_result.get("competitive_price")
            if competitor_name and competitor_price is not None:
                log_parts.append(f"- GiaSosanh: {competitor_name} = {competitor_price:.6f}\n")

            price_min_str = f"{payload.fetched_min_price:.6f}" if payload.fetched_min_price is not None else "None"
            price_max_str = f"{payload.fetched_max_price:.6f}" if payload.fetched_max_price is not None else "None"
            log_parts.append(f"PriceMin = {price_min_str}, PriceMax = {price_max_str}\n")

            sellers_below = analysis_result.get("sellers_below_min", [])
            if sellers_below:
                sellers_info = "; ".join([f"{s.seller_name} = {s.get_price():.6f}\n" for s in sellers_below])
                log_parts.append(f"Seller bỏ qua (Trong blacklist/giá nhỏ hơn):\n {sellers_info}")

            log_parts.append("Top 4 sản phẩm:\n")
            sorted_product = sorted(filtered_products, key=lambda item: item.price, reverse=True)
            for product in sorted_product[:4]:
                log_parts.append(f"- {product.name} ({product.seller_name}): {product.get_price():.6f}\n")

        return " ".join(log_parts)

    return ""
