# models/sheet_models.py
import logging
from typing import Annotated, List, Optional, ClassVar, Type, Dict, Any

from pydantic import BaseModel, ValidationError, computed_field


def _col_to_index(col_name: str) -> int:
    """Chuyển đổi tên cột của Google Sheet (ví dụ: 'A', 'B', 'AA') thành chỉ số bắt đầu từ 0."""
    index = 0
    for char in col_name.upper():
        index = index * 26 + (ord(char) - ord('A') + 1)
    return index - 1


class BaseGSheetModel(BaseModel):
    """
    Model cơ sở với logic để phân tích một hàng từ Google Sheet
    dựa trên metadata 'Annotated' chứa tên cột. (ĐÃ SỬA LỖI)
    """
    row_index: int

    # Class variables để lưu cache bản đồ ánh xạ
    _index_map: ClassVar[Optional[Dict[str, int]]] = None
    _col_map: ClassVar[Optional[Dict[str, str]]] = None

    @classmethod
    def _build_maps_if_needed(cls):
        """
        Xây dựng bản đồ ánh xạ nếu chúng chưa được tạo.
        Đây là phương pháp mạnh mẽ hơn __init_subclass__.
        """
        # Chỉ xây dựng một lần duy nhất
        if cls._index_map is not None and cls._col_map is not None:
            return

        # logging.info(f"Lần đầu tiên: Đang xây dựng bản đồ cột cho model {cls.__name__}...")
        index_map = {}
        col_map = {}
        for field_name, field_info in cls.model_fields.items():
            # Bỏ qua các trường không có metadata (như row_index và computed_field)
            if not field_info.metadata:
                continue

            column_letter = field_info.metadata[0]
            if isinstance(column_letter, str):
                index_map[field_name] = _col_to_index(column_letter)
                col_map[field_name] = column_letter

        cls._index_map = index_map
        cls._col_map = col_map
        # logging.info("-> Xây dựng bản đồ cột hoàn tất.")

    @classmethod
    def from_row(cls, row_data: List[str], row_index: int) -> Optional['BaseGSheetModel']:
        """
        Phương thức Factory để tạo một instance model từ dữ liệu một hàng.
        """
        # Đảm bảo bản đồ cột đã được xây dựng trước khi sử dụng
        cls._build_maps_if_needed()

        data_dict = {}
        # Sử dụng bản đồ đã được xây dựng
        for field_name, col_index in cls._index_map.items():
            if col_index < len(row_data):
                value = row_data[col_index]
                # Gán giá trị rỗng hoặc None thay vì chuỗi rỗng để validation tốt hơn
                data_dict[field_name] = value if value != '' else None

        # Bỏ qua nếu không có dữ liệu gì trong hàng được ánh xạ
        if not any(data_dict.values()):
            return None

        data_dict['row_index'] = row_index

        try:
            return cls.model_validate(data_dict)
        except ValidationError as e:
            product_name_index = cls._index_map.get("product_name")
            name_for_log = ""
            if product_name_index is not None and product_name_index < len(row_data):
                name_for_log = row_data[product_name_index] or f"Hàng {row_index}"
            else:
                name_for_log = f"Hàng {row_index}"

            # logging.warning(f"Bỏ qua '{name_for_log}' (hàng {row_index}) do lỗi xác thực: {e}")
            return None

class SheetLocation(BaseModel):
    """Model lồng nhau để đại diện cho một vị trí trong Sheet."""
    sheet_id: Optional[str] = None
    sheet_name: Optional[str] = None
    cell: Optional[str] = None


class Payload(BaseGSheetModel):
    """
    Model chính để đọc dữ liệu sản phẩm, sử dụng 'Annotated' để ánh xạ cột.
    """
    is_2lai_enabled_str: Annotated[Optional[str], "A"] = None
    is_check_enabled_str: Annotated[Optional[str], "B"] = None
    product_name: Annotated[str, "C"]
    note: Annotated[Optional[str], "D"] = None
    last_update: Annotated[Optional[str], "E"] = None
    product_id: Annotated[Optional[int], "F"] = None
    product_variant_id: Annotated[Optional[int], "G"] = None
    is_compare_enabled_str: Annotated[Optional[str], "H"] = None
    product_compare: Annotated[Optional[str], "I"] = None
    min_price_adjustment: Annotated[Optional[float], "J"] = None
    max_price_adjustment: Annotated[Optional[float], "K"] = None
    price_rounding: Annotated[Optional[int], "L"] = None
    order_sold: Annotated[Optional[int], "M"] = None
    idsheet_min: Annotated[Optional[str], "N"] = None
    sheet_min: Annotated[Optional[str], "O"] = None
    cell_min: Annotated[Optional[str], "P"] = None
    idsheet_max: Annotated[Optional[str], "Q"] = None
    sheet_max: Annotated[Optional[str], "R"] = None
    cell_max: Annotated[Optional[str], "S"] = None
    idsheet_stock: Annotated[Optional[str], "T"] = None
    sheet_stock: Annotated[Optional[str], "U"] = None
    cell_stock: Annotated[Optional[str], "V"] = None
    idsheet_blacklist: Annotated[Optional[str], "W"] = None
    sheet_blacklist: Annotated[Optional[str], "X"] = None
    cell_blacklist: Annotated[Optional[str], "Y"] = None
    relax: Annotated[Optional[str], "Z"] = None
    include_keyword: Annotated[Optional[str], "AA"] = None
    exclude_keyword: Annotated[Optional[str], "AB"] = None

    @computed_field
    @property
    def min_price_location(self) -> SheetLocation:
        return SheetLocation(sheet_id=self.idsheet_min, sheet_name=self.sheet_min, cell=self.cell_min)

    @computed_field
    @property
    def max_price_location(self) -> SheetLocation:
        return SheetLocation(sheet_id=self.idsheet_max, sheet_name=self.sheet_max, cell=self.cell_max)

    @computed_field
    @property
    def stock_location(self) -> SheetLocation:
        return SheetLocation(sheet_id=self.idsheet_stock, sheet_name=self.sheet_stock, cell=self.cell_stock)

    @computed_field
    @property
    def blacklist_location(self) -> SheetLocation:
        return SheetLocation(sheet_id=self.idsheet_blacklist, sheet_name=self.sheet_blacklist, cell=self.cell_blacklist)

    @property
    def is_check_enabled(self) -> bool:
        return self.is_check_enabled_str == '1'

    @property
    def is_2lai_enabled(self) -> bool:
        return self.is_2lai_enabled_str == '1'

    @property
    def is_compare_enabled(self) -> bool:
        return self.is_compare_enabled_str == '1'


    def prepare_update(self, sheet_name: str, updates: Dict[str, Any]) -> List[Dict]:
        """
        Tạo danh sách các yêu cầu cập nhật cho API batchUpdate.

        Args:
            sheet_name (str): Tên của sheet để xây dựng dải ô (ví dụ: 'Gamivo').
            updates (Dict[str, Any]): Một dictionary với key là tên trường model
                                     (ví dụ: 'note') và value là giá trị mới.

        Returns:
            List[Dict]: Một danh sách các dictionary, sẵn sàng để gửi đi.
                        Ví dụ: [{'range': 'Gamivo!D50', 'values': [['Giá trị mới']]}]
        """
        update_requests = []
        for field_name, new_value in updates.items():
            # Lấy chữ cái của cột từ metadata đã lưu
            column_letter = self._col_map.get(field_name)

            if not column_letter:
                logging.warning(f"Trường '{field_name}' không được định nghĩa để cập nhật trong model.")
                continue

            # Xây dựng dải ô A1, ví dụ: 'Gamivo!D50'
            cell_range = f"{sheet_name}!{column_letter}{self.row_index}"

            # Tạo payload cho yêu cầu cập nhật
            update_requests.append({
                'range': cell_range,
                'values': [[str(new_value)]]  # API yêu cầu một list của list
            })

        return update_requests
