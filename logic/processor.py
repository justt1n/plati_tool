# logic/processor.py
import logging
import time
from datetime import datetime
from typing import Dict, Any, List

import requests

from models.digiseller_models import BsProduct
from models.sheet_models import Payload
from services.digiseller_service import get_product_list
from utils.config import settings


def process_single_payload(payload: Payload) -> Dict[str, Any]:
    logging.info(f"Processing payload for product: {payload.product_name} (Row: {payload.row_index})")
    html_str = requests.get(payload.product_compare).text
    min_price = get_min_price(html_str, payload.product_compare2)

    current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    result = {
        'note': f"{payload.model_dump_json()}",
        'last_update': current_time_str
    }

    logging.info(f"-> Xử lý nghiệp vụ hoàn tất cho sản phẩm: {payload.product_name}")
    return result


def get_min_price(html_str: str, payload: Payload) -> Dict[str, Any]:
    product_list = get_product_list(html_str)

    if not product_list:
        logging.warning("No products found in the provided link")
        return {}

    # calc sub products
    #TODO main flow
    min_price = find_min_price_in_range(payload, product_list)

    return {}


def find_min_price_in_range(payload: Payload, product_list: List[BsProduct]):
    """
    Get the closest price to the product in the product_list base on min max price of payload
    :param payload:
    :param product_list:
    :return:
    """
    min_price = payload.fetched_min_price
    max_price = payload.fetched_max_price

    closest_product = None
    #TODO: Implement logic to find the closest product based on min and max price
    for p in product_list:
        if p.price >= min_price:
            closest_product = p
            break
    return {
        'closest_product': closest_product,
        'min_price': closest_product.fetched_min_price if closest_product else None,
        'max_price': closest_product.fetched_max_price if closest_product else None
    }
