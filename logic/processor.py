# logic/processor.py
import logging
import time
from datetime import datetime
from typing import Dict, Any

from models.sheet_models import Payload


def process_single_payload(payload: Payload) -> Dict[str, Any]:
    """
    Thực hiện logic nghiệp vụ chính cho một payload.
    Hàm này không biết gì về Google Sheets, nó chỉ nhận dữ liệu,
    xử lý và trả về kết quả dưới dạng dictionary.

    Args:
        payload (Payload): Đối tượng payload chứa dữ liệu của một hàng.

    Returns:
        Dict[str, Any]: Một dictionary chứa các trường cần cập nhật lại.
                        Ví dụ: {'note': '...', 'last_update': '...'}
    """
    logging.info(f"Bắt đầu xử lý nghiệp vụ cho sản phẩm: {payload.product_name}")

    # --- Đặt logic nghiệp vụ phức tạp của bạn ở đây ---
    # Ví dụ:
    # 1. Gọi API của Gamivo hoặc các trang khác để lấy giá.
    # 2. So sánh giá, áp dụng các quy tắc trong payload.
    # 3. Tính toán giá mới.
    # 4. Giả lập một tác vụ tốn thời gian.
    time.sleep(0.2)

    # Sau khi xử lý, tạo kết quả để ghi log
    current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    result = {
        'note': f"Xử lý thành công.",
        'last_update': current_time_str
    }

    logging.info(f"-> Xử lý nghiệp vụ hoàn tất cho sản phẩm: {payload.product_name}")
    return result
