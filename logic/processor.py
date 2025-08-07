# logic/processor.py
import logging
import random
from datetime import datetime
from typing import Dict, Any, List

import requests

from models.digiseller_models import BsProduct
from models.sheet_models import Payload
from services.digiseller_service import get_product_list, analyze_product_offers


async def process_single_payload(payload: Payload) -> Dict[str, Any]:
    logging.info(f"Processing payload for product: {payload.product_name} (Row: {payload.row_index})")
    html_str = requests.get(payload.product_compare).text
    product_list = await get_product_list(html_str, payload.product_compare2)
    if not product_list:
        logging.warning("No products found in the provided link")
        return {}
    product_list = product_list[:8]
    filtered_product_list = filter_products(product_list, payload)

    analysis_result = analyze_product_offers(
        offers=filtered_product_list,
        min_price=payload.fetched_min_price,
        black_list=payload.fetched_blacklist
    )
    final_price = calc_final_price(
        price=analysis_result['competitive_price'],
        payload=payload
    )
    edit_product_price(final_price, payload)

    current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    result = {
        'note': f"{payload.model_dump_json()}",
        'last_update': current_time_str
    }

    return result


def filter_products(products: List[BsProduct], payload: Payload) -> List[BsProduct]:
    filtered_products = []
    for product in products:
        if payload.include_keyword is not None and payload.include_keyword not in product.name:
            continue
        if payload.exclude_keyword is not None and payload.exclude_keyword in product.name:
            continue
        if product.sold_count < payload.order_sold:
            continue
        filtered_products.append(product)

    return filtered_products


def edit_product_price(price: float, payload: Payload) -> float:
    # call the API to edit the product price
    pass


def calc_final_price(price: float, payload: Payload) -> float:
    if payload.min_price_adjustment is None and payload.max_price_adjustment is None:
        return price
    d_price = random.uniform(payload.min_price_adjustment, payload.max_price_adjustment)
    price = price - d_price
    if payload.price_rounding is not None:
        price = round(price, payload.price_rounding)
    return price
