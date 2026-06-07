# -*- coding: utf-8 -*-
"""
Chương trình Demo chính (CLI) - Thuần Client (Pure Client)
==============================================
Vai trò của file:
    File `main.py` đóng vai trò là Lớp Giao diện Người dùng (User Interface Layer) hay "Client"
    trong mô hình Client-Server của hệ thống quản trị cơ sở dữ liệu phân tán (Distributed DBMS).
    Nhiệm vụ duy nhất của file này là hiển thị Menu, nhận lệnh từ người dùng, và gửi các lệnh đó 
    qua mạng (HTTP) tới Master Node (Site 0). Nó KHÔNG chứa bất kỳ logic xử lý dữ liệu, 
    không kết nối trực tiếp vào CSDL, và cũng KHÔNG biết dữ liệu thực sự nằm ở đâu.

Mục đích:
    Chứng minh "Tính trong suốt phân tán" (Distribution Transparency) - Một trong những nguyên lý 
    tối thượng của CSDL phân tán. Người dùng/Client chỉ tương tác với 1 điểm duy nhất (Master Node), 
    và hệ thống ngầm xử lý mọi sự phức tạp bên dưới (phân mảnh, truy vấn qua mạng, gộp dữ liệu).
"""

import time          # Dùng để đo lường thời gian (nếu cần) hoặc tạm dừng (sleep)
import sys           # Dùng để can thiệp vào hệ thống (ví dụ: đổi bảng mã hiển thị terminal)
import json          # Dùng để xử lý (phân tích và tạo) các chuỗi định dạng JSON (mặc dù requests đã hỗ trợ phần lớn)
import requests      # Thư viện cực kỳ quan trọng dùng để gửi các HTTP Request (GET, POST) qua mạng tới các Node

# Khắc phục lỗi hiển thị tiếng Việt (mã hóa cp1252) trên môi trường dòng lệnh (Terminal/CMD) của Windows
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Import hàm deserialize_object từ src.models để biến chuỗi JSON thô thành đối tượng Python (Object Rehydration tại Client)
from src.models import deserialize_object

# =============================================================================
# CÁC BIẾN TOÀN CỤC (GLOBAL VARIABLES)
# =============================================================================

# Biến BANNER: Chuỗi văn bản hiển thị tiêu đề hoành tráng khi vừa mở chương trình
BANNER = """
+--------------------------------------------------------------+
|   KẾ THỪA PHÂN TÁN: DEMO VEHICLE FLEET (THỰC TẾ NGHIỆP VỤ)   |
|   Đề tài #89 -- Ozsu & Valduriez, Phiên bản 4                |
+--------------------------------------------------------------+
Tính trong suốt phân tán (Distribution Transparency):
  Client này KHÔNG tự kết nối đến Site 1 hay Site 2.
  Client chỉ gọi duy nhất Master Node tại: http://localhost:5000
"""

# Biến MENU: Chuỗi văn bản hiển thị danh sách các chức năng cho người dùng chọn
MENU = """
---------------------------------------------
  MENU CHÍNH (CLIENT)
---------------------------------------------
  [1] Kiểm tra trạng thái Master Node
  [2] Tìm kiếm Đa hình -- TẤT CẢ các xe
  [3] Tìm kiếm Đa hình -- Lọc theo hãng sản xuất (make)
  [4] Tìm kiếm Đa hình -- Lọc theo khoảng năm sản xuất
  [5] Chứng minh chi phí Phục hồi Đối tượng (Rehydration Overhead)
  [6] Tiến hóa Lược đồ (Schema Evolution) -- Thêm thuộc tính mới
  [7] Khảo sát Hệ thống qua Master Node (Xem thống kê)
  [0] Thoát
---------------------------------------------
"""

# Biến MASTER_NODE: Lưu trữ địa chỉ mạng của Node Điều phối (Coordinator / Site 0). 
# Toàn bộ mã nguồn Client chỉ gửi request tới đúng 1 địa chỉ này.
MASTER_NODE = "http://localhost:5000"


# =============================================================================
# CÁC HÀM TIỆN ÍCH GIAO TIẾP MẠNG (NETWORK UTILITY FUNCTIONS)
# =============================================================================

def get_global(path, params=None):
    """
    Hàm tiện ích gửi HTTP GET Request tới Master Node.
    
    Tham số:
        path (str): Đường dẫn API (ví dụ: '/global/search').
        params (dict): Các tham số truyền trên URL (ví dụ: ?field=make&value=Tesla).
        
    Trả về:
        dict: Dữ liệu JSON từ server trả về nếu thành công. Ngược lại trả về None.
    """
    try:
        # Gửi request với thời gian chờ tối đa (timeout) là 10 giây
        return requests.get(f"{MASTER_NODE}{path}", params=params, timeout=10.0).json()
    except requests.exceptions.ConnectionError:
        # Xử lý lỗi nếu Master Node chưa được bật hoặc bị sập
        print("\n[LỖI CẢNH BÁO] Không thể kết nối tới Master Node (Site 0). Hãy đảm bảo bạn đã chạy docker compose!")
        return None

def post_global(path, json_data):
    """
    Hàm tiện ích gửi HTTP POST Request tới Master Node (Dùng khi cần gửi dữ liệu đi, ví dụ: cập nhật lược đồ).
    
    Tham số:
        path (str): Đường dẫn API (ví dụ: '/global/schema_evolve').
        json_data (dict): Dữ liệu gói trong body của request.
        
    Trả về:
        dict: Dữ liệu JSON từ server trả về.
    """
    try:
        return requests.post(f"{MASTER_NODE}{path}", json=json_data, timeout=10.0).json()
    except requests.exceptions.ConnectionError:
        print("\n[LỖI CẢNH BÁO] Không thể kết nối tới Master Node (Site 0).")
        return None

def print_objects(raw_objects, max_show=20):
    """
    Hàm tiện ích để in danh sách các đối tượng ra màn hình một cách đẹp mắt.
    
    Tham số:
        raw_objects (list): Danh sách các dictionary JSON nhận được từ Server.
        max_show (int): Số lượng đối tượng tối đa in ra màn hình để tránh trôi Terminal.
    """
    # Giải tuần tự hóa (Deserialize): Biến các dict JSON thành các Object Python thực sự (Vehicle, Truck, ElectricCar)
    objects = [deserialize_object(o) for o in raw_objects]
    print(f"\n  Đã tìm thấy {len(objects)} đối tượng:\n")
    
    # In ra thông tin của từng đối tượng thông qua phương thức display() của lớp
    for obj in objects[:max_show]:
        print(obj.display())
        print()
        
    # Báo cho người dùng nếu kết quả quá dài bị cắt bớt
    if len(objects) > max_show:
        print(f"  ... và {len(objects) - max_show} đối tượng khác.")


# =============================================================================
# CÁC HÀM XỬ LÝ NGHIỆP VỤ (BUSINESS LOGIC HANDLERS)
# =============================================================================

def demo_ping():
    """Chức năng 1: Bắn tín hiệu (Ping) tới Master Node để kiểm tra xem hệ thống có đang sống không."""
    print("\n[PING MASTER NODE] Đang kiểm tra kết nối tới http://localhost:5000...")
    data = get_global("/ping")
    if data:
        # In ra tên của Site lấy từ cục JSON trả về
        print(f"  [OK] Master Node trực tuyến: {data.get('site_name')}")


def demo_polymorphic_all():
    """Chức năng 2: Tìm kiếm Đa hình. Lấy TẤT CẢ các đối tượng siêu lớp (Vehicle) và các lớp con (Truck, EV)."""
    print("\n[TÌM KIẾM ĐA HÌNH] Đang gọi Master Node để truy vấn TẤT CẢ các xe...")
    data = get_global("/global/search")
    if data:
        print_objects(data["objects"])
        print("\n" + data.get("summary_text", ""))


def demo_polymorphic_filter():
    """Chức năng 3: Tìm kiếm có chọn lọc (Lọc theo hãng sản xuất)."""
    # Yêu cầu người dùng nhập tên hãng, nếu ấn Enter không nhập gì thì mặc định là 'Tesla'
    make = input("  Nhập hãng sản xuất để tìm kiếm (ví dụ: Tesla, Volvo): ").strip()
    if not make:
        make = "Tesla"
    print(f"\n[TÌM KIẾM ĐA HÌNH] Đang gọi Master Node để truy vấn xe có hãng sản xuất='{make}'...")
    
    # Đính kèm tham số `field` và `value` vào URL GET request
    data = get_global("/global/search", {"field": "make", "value": make})
    if data:
        print_objects(data["objects"])
        print("\n" + data.get("summary_text", ""))


def demo_polymorphic_year():
    """Chức năng 4: Tìm kiếm theo khoảng thời gian (Từ năm - Đến năm)."""
    try:
        # Lấy năm từ người dùng, ép kiểu về số nguyên (int). Mặc định là 2021-2023.
        year_min = int(input("  Từ năm (ví dụ: 2021): ").strip() or "2021")
        year_max = int(input("  Đến năm (ví dụ: 2023): ").strip() or "2023")
    except ValueError:
        # Nếu người dùng nhập chữ (sai định dạng), tự gán giá trị mặc định
        year_min, year_max = 2021, 2023
        
    print(f"\n[TÌM KIẾM ĐA HÌNH] Đang gọi Master Node truy vấn xe sản xuất năm {year_min}-{year_max}...")
    data = get_global("/global/search", {"year_min": year_min, "year_max": year_max})
    if data:
        print_objects(data["objects"])
        print("\n" + data.get("summary_text", ""))


def demo_rehydration_cost():
    """
    Chức năng 5: Khái niệm Cực kỳ Quan trọng - Đo lường chi phí mạng.
    Hàm này so sánh thời gian truy xuất dữ liệu nếu chỉ lấy ở CSDL Cục bộ (Site 0) 
    so với việc phải phân tán (gọi mạng sang Site 1, Site 2) rồi "khâu" dữ liệu lại (Rehydration).
    """
    print("\n[PHÂN TÍCH CHI PHÍ PHỤC HỒI ĐỐI TƯỢNG (THỰC TẾ MẠNG)]")
    print("  Đang so sánh chi phí khi Master Node CHỈ truy cập local DB vs. Gọi toàn bộ mạng")

    # BƯỚC 1: Chỉ lấy từ Site 0
    print("\n  Bước 1: Master Node CHỈ truy vấn Local (Không phục hồi qua mạng)...")
    data_base = get_global("/global/search", {"include_sites": "0"})
    if not data_base: return
    t_base = data_base["total_time"] # Thời gian thực thi do server trả về
    print(f"  -> {len(data_base['objects'])} đối tượng trong {t_base:.4f}s")

    # BƯỚC 2: Bắt Server phải lấy từ tất cả các Site (0, 1, 2)
    print("\n  Bước 2: Master Node truy vấn TẤT CẢ các site (Phục hồi qua mạng)...")
    data_full = get_global("/global/search", {"include_sites": "0,1,2"})
    t_full = data_full["total_time"]
    print(f"  -> {len(data_full['objects'])} đối tượng trong {t_full:.4f}s (đã phục hồi {data_full['rehydration_count']})")

    # TÍNH TOÁN ĐỘ TRỄ MẠNG (OVERHEAD) Bằng mili-giây (ms)
    overhead = (t_full - t_base) * 1000
    print(f"\n  CHI PHÍ MẠNG (NETWORK OVERHEAD): +{overhead:.2f}ms")
    print("  (Lưu ý: Do các site chạy trên cùng mạng ảo Docker nội bộ, độ trễ thực tế cực thấp.")
    print("   Trong môi trường Internet/Cloud thực tế, con số này có thể từ 10-50ms)")


def demo_schema_evolution():
    """
    Chức năng 6: Tiến hóa Lược đồ (Schema Evolution) - Thêm thuộc tính lúc runtime (đang chạy).
    Hàm này bắn 1 POST request tới Master Node. Master Node sẽ chịu trách nhiệm
    'Loan báo' (Broadcast) thuộc tính mới này tới toàn bộ mạng lưới các DB.
    """
    print("\n[DEMO TIẾN HÓA LƯỢC ĐỒ QUA MASTER NODE]")
    print("  Client chỉ ra lệnh cho Master Node, Master Node tự động broadcast cập nhật.")

    attr = input("  Tên thuộc tính mới (mặc định: fuel_type): ").strip() or "fuel_type"
    default = input("  Giá trị mặc định (mặc định: 'Diesel'): ").strip() or "Diesel"
    version = input("  Phiên bản lược đồ mới (mặc định: 1.1.0): ").strip() or "1.1.0"

    results = post_global("/global/schema_evolve", {
        "attribute": attr,
        "default": default,
        "new_version": version
    })
    
    if results:
        print("\n  KẾT QUẢ TỪ MASTER NODE:")
        for sid, res in results.items():
            print(f"    - Site {sid}: {res}")


def demo_oid_stats():
    """Chức năng 7: Lấy các báo cáo thống kê quản trị từ Master Node."""
    print("\n[THỐNG KÊ QUẢN LÝ TỪ MASTER NODE]")
    stats = get_global("/global/stats")
    if stats:
        for site_id, data in stats.items():
            print(f"\n  Site {site_id}:")
            if "error" in data:
                print(f"    [LỖI] {data['error']}")
            else:
                print(f"    Số đối tượng: {data.get('object_count', 0)}")
                print(f"    Bộ đếm OID  : {data.get('oid_stats', {})}")


# =============================================================================
# HÀM MAIN (HÀM CHÍNH ĐIỀU HƯỚNG VÒNG LẶP MENU)
# =============================================================================

def main():
    """Hàm trung tâm, thiết lập vòng lặp vô hạn (while True) để giữ chương trình chạy mãi cho tới khi ấn 0."""
    print(BANNER)

    # Từ điển (Dictionary) ánh xạ phím người dùng bấm sang các hàm tương ứng
    handlers = {
        "1": demo_ping,
        "2": demo_polymorphic_all,
        "3": demo_polymorphic_filter,
        "4": demo_polymorphic_year,
        "5": demo_rehydration_cost,
        "6": demo_schema_evolution,
        "7": demo_oid_stats,
    }

    # Vòng lặp chương trình
    while True:
        print(MENU)
        choice = input("  Nhập lựa chọn: ").strip()
        
        # Thoát nếu người dùng chọn 0
        if choice == "0":
            print("\nĐang thoát Client. Tạm biệt!\n")
            break
            
        # Lấy hàm ra từ dictionary dựa trên phím vừa gõ
        handler = handlers.get(choice)
        if handler:
            try:
                handler() # Thực thi hàm
            except Exception as exc:
                print(f"\n  [LỖI CLIENT] {exc}")
        else:
            print("  Lựa chọn không hợp lệ.")
            
        input("\n  [Nhấn Enter để tiếp tục]")


# Đoạn code chuẩn của Python để xác định xem file này đang được chạy trực tiếp 
# hay bị import bởi một file khác. Nếu chạy trực tiếp thì mới gọi hàm main().
if __name__ == "__main__":
    main()
