"""
Mô hình đối tượng (Object Model) - Lõi của CSDL Hướng đối tượng phân tán (OODBMS)
==================================================================================
Vai trò của file:
    File `models.py` định nghĩa "Lược đồ dữ liệu ảo" (Logical Schema) của toàn hệ thống.
    Thay vì dùng các lệnh CREATE TABLE với các cột khô khan như SQL truyền thống, 
    OODBMS định nghĩa dữ liệu bằng chính các Lớp (Class) lập trình hướng đối tượng.
    File này thiết lập cấu trúc Kế thừa: Lớp cha (Vehicle) mang những đặc tính chung,
    Lớp con (Truck, ElectricCar) mang những đặc tính riêng mở rộng từ lớp cha.

    File này sử dụng thư viện MARSHMALLOW để thực hiện quá trình TUẦN TỰ HÓA (Serialization)
    và GIẢI TUẦN TỰ HÓA (Deserialization) một cách trực quan, đáp ứng đúng yêu cầu của đồ án.

Dựa trên lý thuyết:
    Özsu & Valduriez "Principles of Distributed Database Systems" (4th Ed.)
    - The Object Model (Lớp, thuộc tính, kế thừa)
    - Schema Evolution in Distributed OODBs (Tiến hóa lược đồ)
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

# Thư viện Marshmallow để tự động hóa Object-to-Data transformations
from marshmallow import Schema, fields, post_load, INCLUDE

from src.oid_manager import OID
from src.config import SCHEMA_VERSION

# =============================================================================
# MARSHMALLOW CUSTOM FIELDS (CÁC TRƯỜNG DỮ LIỆU TÙY CHỈNH)
# =============================================================================

class OIDField(fields.Field):
    """
    Trường tùy chỉnh của Marshmallow để xử lý kiểu dữ liệu OID.
    Mục đích: 
      - Khi đẩy qua mạng (Serialize): Ép Object OID thành chuỗi String (VD: "0.Vehicle.1")
      - Khi nhận từ mạng (Deserialize): Ép chuỗi String ngược lại thành Object OID
    """
    def _serialize(self, value: Any, attr: str, obj: Any, **kwargs):
        if value is None:
            return ""
        return str(value)

    def _deserialize(self, value: Any, attr: str, data: Dict[str, Any], **kwargs):
        if not value:
            raise ValueError("Thiếu giá trị OID")
        return OID.from_string(value)


# =============================================================================
# LỚP CƠ SỞ (BASE CLASS) - VEHICLE
# =============================================================================

@dataclass
class Vehicle:
    """Siêu lớp (Superclass) - Lưu trữ các thông tin cơ bản tại Site 0."""
    oid: OID            
    make: str           
    model: str          
    year: int           
    vin: str            
    schema_version: str = field(default=SCHEMA_VERSION)
    
    # Biến nội bộ (Không gửi qua mạng)
    _ref_count: int = field(default=0, repr=False, compare=False)
    _created_at: float = field(default_factory=time.time, repr=False, compare=False)

    def display(self) -> str:
        base = (
            f"[Vehicle] OID={self.oid}  VIN={self.vin}\n"
            f"  Hãng/Mẫu: {self.make} {self.model} ({self.year})\n"
            f"  Phiên bản Schema: v{self.schema_version}"
        )
        
        dynamic_attrs = []
        for k, v in self.__dict__.items():
            if k not in ['oid', 'make', 'model', 'year', 'vin', 'schema_version'] and not k.startswith('_'):
                dynamic_attrs.append(f"{k}={v}")
        if dynamic_attrs:
            base += "\n  [Thuộc tính mở rộng] " + " | ".join(dynamic_attrs)
            
        return base

class VehicleSchema(Schema):
    """
    Lược đồ Marshmallow cho lớp Vehicle.
    Khai báo chính xác kiểu dữ liệu của từng cột để thư viện tự động ép kiểu.
    """
    # INCLUDE cho phép Marshmallow nạp các thuộc tính lạ (Schema Evolution) không bị lỗi
    class Meta:
        unknown = INCLUDE

    # Thêm nhãn '__class__' để hỗ trợ tính đa hình
    cls_name = fields.String(data_key="__class__", attribute="__class__.__name__", dump_only=True)
    oid = OIDField(required=True)
    make = fields.String(required=True)
    model = fields.String(required=True)
    year = fields.Integer(required=True)
    vin = fields.String(required=True)
    schema_version = fields.String(load_default="1.0.0")

    @post_load
    def make_object(self, data, **kwargs):
        """Hàm này tự động chạy sau khi Marshmallow đọc xong JSON để đúc ra Object."""
        data.pop("__class__", None)
        
        # Tách riêng các trường cốt lõi và các trường mở rộng
        core_fields = ['oid', 'make', 'model', 'year', 'vin', 'schema_version']
        core_data = {k: v for k, v in data.items() if k in core_fields}
        extra_data = {k: v for k, v in data.items() if k not in core_fields}
        
        # Đúc xe cơ bản
        obj = Vehicle(**core_data)
        
        # Gắn thêm các tính năng mở rộng
        for k, v in extra_data.items():
            setattr(obj, k, v)
            
        return obj


# =============================================================================
# LỚP CON (SUBCLASS) - TRUCK (Kế thừa từ Vehicle)
# =============================================================================

@dataclass
class Truck(Vehicle):
    """Lớp xe tải. Lưu trữ tại Site 1."""
    payload_capacity_kg: float = 0.0   
    axle_count: int = 2                
    has_trailer: bool = False          

    def display(self) -> str:
        base = super().display().replace("[Vehicle]", "[Truck   ]")
        out = (
            base + "\n"
            f"  Tải trọng: {self.payload_capacity_kg:,.0f} kg  "
            f"Số trục: {self.axle_count}  "
            f"Rơ-moóc: {'Có' if self.has_trailer else 'Không'}"
        )
        
        dynamic_attrs = []
        for k, v in self.__dict__.items():
            if k not in ['oid', 'make', 'model', 'year', 'vin', 'schema_version', 'payload_capacity_kg', 'axle_count', 'has_trailer'] and not k.startswith('_'):
                dynamic_attrs.append(f"{k}={v}")
        if dynamic_attrs:
            out += "\n  [Thuộc tính mở rộng] " + " | ".join(dynamic_attrs)
            
        return out

class TruckSchema(VehicleSchema):
    """
    Kế thừa VehicleSchema, bổ sung thêm các cột riêng của Xe tải.
    """
    cls_name = fields.String(data_key="__class__", attribute="__class__.__name__", dump_only=True)
    payload_capacity_kg = fields.Float(load_default=0.0)
    axle_count = fields.Integer(load_default=2)
    has_trailer = fields.Boolean(load_default=False)

    @post_load
    def make_object(self, data, **kwargs):
        data.pop("__class__", None)
        
        # Tách riêng các trường cốt lõi và các trường mở rộng
        core_fields = ['oid', 'make', 'model', 'year', 'vin', 'schema_version', 'payload_capacity_kg', 'axle_count', 'has_trailer']
        core_data = {k: v for k, v in data.items() if k in core_fields}
        extra_data = {k: v for k, v in data.items() if k not in core_fields}
        
        # Đúc xe tải
        obj = Truck(**core_data)
        
        # Gắn thêm các tính năng mở rộng
        for k, v in extra_data.items():
            setattr(obj, k, v)
            
        return obj


# =============================================================================
# LỚP CON (SUBCLASS) - ELECTRIC CAR (Kế thừa từ Vehicle)
# =============================================================================

@dataclass
class ElectricCar(Vehicle):
    """Lớp xe điện. Lưu trữ tại Site 2."""
    battery_capacity_kwh: float = 0.0  
    range_km: int = 0                  
    charge_connector: str = "Type2"    

    def display(self) -> str:
        base = super().display().replace("[Vehicle]", "[EV Car  ]")
        out = base + "\n" + f"  Pin: {self.battery_capacity_kwh} kWh  Tầm xa: {self.range_km} km  Cổng sạc: {self.charge_connector}"
        
        dynamic_attrs = []
        for k, v in self.__dict__.items():
            if k not in ['oid', 'make', 'model', 'year', 'vin', 'schema_version', 'battery_capacity_kwh', 'range_km', 'charge_connector'] and not k.startswith('_'):
                dynamic_attrs.append(f"{k}={v}")
        if dynamic_attrs:
            out += "\n  (Thuộc tính mở rộng): " + ", ".join(dynamic_attrs)
        return out

class ElectricCarSchema(VehicleSchema):
    """
    Kế thừa VehicleSchema, bổ sung thêm các cột riêng của Xe điện.
    """
    cls_name = fields.String(data_key="__class__", attribute="__class__.__name__", dump_only=True)
    battery_capacity_kwh = fields.Float(load_default=0.0)
    range_km = fields.Integer(load_default=0)
    charge_connector = fields.String(load_default="Unknown")

    @post_load
    def make_object(self, data, **kwargs):
        data.pop("__class__", None)
        # Bóc tách các thuộc tính cứng
        core_fields = ['oid', 'make', 'model', 'year', 'vin', 'schema_version', 'battery_capacity_kwh', 'range_km', 'charge_connector']
        core_data = {k: v for k, v in data.items() if k in core_fields}
        
        # Khởi tạo Object
        obj = ElectricCar(**core_data)
        
        # Dán thêm các thuộc tính tiến hóa (Schema Evolution) nếu có
        for k, v in data.items():
            if k not in core_fields:
                setattr(obj, k, v)
        return obj


# =============================================================================
# HÀM NHÀ MÁY (FACTORY) - KẾT NỐI VÀ TÁI TẠO ĐA HÌNH
# =============================================================================

# Khởi tạo các Schema Object của Marshmallow
vehicle_schema = VehicleSchema()
truck_schema = TruckSchema()
ev_schema = ElectricCarSchema()

SCHEMA_MAP = {
    "Vehicle": vehicle_schema,
    "Truck": truck_schema,
    "ElectricCar": ev_schema,
}

CLASS_MAP = {
    "Vehicle": Vehicle,
    "Truck": Truck,
    "ElectricCar": ElectricCar,
}

def deserialize_object(data: Dict[str, Any]) -> Vehicle:
    """
    Phân giải Đa hình (Polymorphism):
    Đọc nhãn "__class__" để biết phải dùng Lược đồ Marshmallow nào để khôi phục (Load).
    """
    cls_name = data.get("__class__", "Vehicle")
    schema = SCHEMA_MAP.get(cls_name)
    
    if schema is None:
        raise ValueError(f"Lỗi: Không nhận diện được Lớp (Class) trong dữ liệu: {cls_name!r}")
        
    return schema.load(data)

def serialize_object(obj: Vehicle) -> Dict[str, Any]:
    """
    Ép Object Python thành JSON Dictionary thông qua Marshmallow (Dump).
    """
    cls_name = obj.__class__.__name__
    schema = SCHEMA_MAP.get(cls_name)
    
    if schema is None:
        raise ValueError(f"Lỗi: Không tìm thấy Schema để tuần tự hóa class: {cls_name!r}")
        
    base_data = schema.dump(obj)
    
    # Ép thêm các thuộc tính mở rộng do Tiến hóa Lược đồ (Schema Evolve)
    for key, value in obj.__dict__.items():
        if key not in base_data and not key.startswith('_'):
            base_data[key] = value
            
    return base_data
