"""
Cấu hình (Config) - Sơ đồ Mạng lưới (Topology) của Hệ thống Vehicle Fleet phân tán
==================================================================================
Vai trò của file:
    File `config.py` đóng vai trò là "Bản đồ toàn cục" (Global Directory) của toàn bộ hệ thống.
    Nó định nghĩa:
    1. Hệ thống có bao nhiêu Site? Địa chỉ IP/Port của từng Site là gì?
    2. Mỗi Site chịu trách nhiệm lưu trữ loại dữ liệu nào (Vehicle, Truck, hay ElectricCar)?
    3. Cấu trúc Kế thừa (Ai là cha, ai là con, thuộc tính gồm những gì)?
    
    Trong CSDL phân tán, file này đại diện cho "Global Conceptual Schema" và "Fragmentation Schema"
    giúp Điều phối viên (Coordinator) biết phải tìm dữ liệu ở đâu mà không cần hỏi từng máy.

Dựa trên lý thuyết:
    - Distributed Database Design (Kỹ thuật phân mảnh dọc - Vertical Fragmentation).
"""

import os # Thư viện chuẩn của Python dùng để tương tác với Hệ điều hành (đọc biến môi trường)

# =============================================================================
# PHÂN GIẢI ĐỊA CHỈ MÁY CHỦ (HOST RESOLUTION) TƯƠNG THÍCH VỚI DOCKER
# =============================================================================
# Trong Docker, các máy chủ không gọi nhau bằng IP mà gọi bằng tên dịch vụ (Service Name).
# Ví dụ: "site0", "site1". Hàm os.environ.get() sẽ lấy giá trị từ Docker Compose.
# Nếu không chạy bằng Docker (chạy ở máy thật), nó sẽ trả về mặc định là "localhost".

_SITE0_HOST = os.environ.get("SITE0_HOST", "localhost")  # Địa chỉ của Site 0 (Master Node)
_SITE1_HOST = os.environ.get("SITE1_HOST", "localhost")  # Địa chỉ của Site 1 (Kho xe tải)
_SITE2_HOST = os.environ.get("SITE2_HOST", "localhost")  # Địa chỉ của Site 2 (Kho xe điện)

# FLASK_HOST quy định dải IP mà API Server sẽ lắng nghe.
# 0.0.0.0 nghĩa là lắng nghe mọi kết nối (bắt buộc khi dùng Docker).
_FLASK_HOST = os.environ.get("FLASK_HOST", "localhost")


# =============================================================================
# ĐỊNH NGHĨA CÁC SITE (FRAGMENTATION SCHEMA)
# =============================================================================
# Biến SITES là một từ điển (Dictionary) chứa toàn bộ cấu hình vật lý của mạng lưới.
SITES = {
    0: {
        "name": "Site-0 (Vehicle - Global)",
        "host": _SITE0_HOST,                                      # Tên miền/IP để các site khác gọi tới
        "flask_host": _FLASK_HOST,                                # IP để bind server
        "port": 5000,                                             # Cổng mạng nội bộ
        "timeout": 2.0,                                           # Thời gian chờ (TTL) mạng: 2 giây. Trễ cực thấp vì đây là Master.
        "class": "Vehicle",                                       # Site này chỉ lưu thông tin Khung gầm chung (Vehicle)
        "db_uri": os.environ.get("DB_URI_0", "postgresql://user:password@localhost:5432/site0_db"), # Chuỗi kết nối CSDL PostgreSQL
    },
    1: {
        "name": "Site-1 (Truck - Depot A)",
        "host": _SITE1_HOST,
        "flask_host": _FLASK_HOST,
        "port": 5001,
        "timeout": 1.0,                                           # Timeout 1.0s theo yêu cầu
        "class": "Truck",                                         # Chỉ lưu dữ liệu đặc thù của Xe tải (Tải trọng, số trục).
        "db_uri": os.environ.get("DB_URI_1", "postgresql://user:password@localhost:5433/site1_db"),
    },
    2: {
        "name": "Site-2 (ElectricCar - EV Hub)",
        "host": _SITE2_HOST,
        "flask_host": _FLASK_HOST,
        "port": 5002,
        "timeout": 1.0,                                           # Timeout 1.0s theo yêu cầu
        "class": "ElectricCar",                                   # Chỉ lưu Pin, tầm hoạt động.
        "db_uri": os.environ.get("DB_URI_2", "postgresql://user:password@localhost:5434/site2_db"),
    },
}

# =============================================================================
# CÁC BIẾN CẤU HÌNH HỆ THỐNG KHÁC
# =============================================================================

# Gán Site 0 làm Điều phối viên (Coordinator). Mọi request phân tán đều do Site 0 gánh vác.
COORDINATOR_SITE = 0

# Định dạng tuần tự hóa (Serialization format) khi truyền dữ liệu qua cáp mạng. 
# Ở đây dùng 'json' để hỗ trợ Schema Evolution linh hoạt, dễ đọc.
SERIALIZATION_FORMAT = "json"   

# Phiên bản Lược đồ hiện tại. Dùng để đối chiếu khi có sự thay đổi cấu trúc bảng (Tiến hóa lược đồ).
SCHEMA_VERSION = "1.0.0"


# =============================================================================
# SIÊU DỮ LIỆU CÂY KẾ THỪA LỚP (GLOBAL CONCEPTUAL SCHEMA)
# =============================================================================
# Biến CLASS_HIERARCHY cho Coordinator biết Class nào nằm ở Site nào, và gồm những thuộc tính gì.
# Việc này mô phỏng "Bảng tra cứu danh mục toàn cục" (Global Directory) trong lý thuyết.
CLASS_HIERARCHY = {
    "Vehicle": {
        "parent": None,         # Vehicle là lớp gốc, không kế thừa ai
        "site": 0,              # Được lưu ở Site 0
        "attributes": ["oid", "make", "model", "year", "vin", "schema_version"], # Dữ liệu Khung gầm
    },
    "Truck": {
        "parent": "Vehicle",    # Kế thừa từ lớp Vehicle
        "site": 1,              # Phần mở rộng được lưu ở Site 1
        "attributes": ["oid", "payload_capacity_kg", "axle_count", "has_trailer", "schema_version"], # Dữ liệu Xe tải
    },
    "ElectricCar": {
        "parent": "Vehicle",    # Kế thừa từ lớp Vehicle
        "site": 2,              # Phần mở rộng được lưu ở Site 2
        "attributes": ["oid", "battery_capacity_kwh", "range_km", "charge_connector", "schema_version"], # Dữ liệu Xe điện
    },
}

# =============================================================================
# BẢNG TỪ ĐIỂN CHỌN DRIVER SITE CHO QUERY PLANNER
# =============================================================================
ATTRIBUTE_SITE = {
    "make": 0, "model": 0, "year": 0, "vin": 0,
    "payload_capacity_kg": 1, "axle_count": 1, "has_trailer": 1,
    "battery_capacity_kwh": 2, "range_km": 2, "charge_connector": 2
}

CLASS_SITE = {"Vehicle": 0, "Truck": 1, "ElectricCar": 2}
