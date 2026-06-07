"""
Trình quản lý OID (Object Identity Manager) - Quản lý Định danh Đối tượng Phân tán
===================================================================================
Vai trò của file:
    Trong CSDL Quan hệ (SQL), bạn dùng Khóa chính (Primary Key - Thường là số tự tăng) để xác định 1 hàng.
    Nhưng trong CSDL Hướng đối tượng phân tán (OODBMS), nếu Site 1 tạo ra ID số 1, và Site 2 cũng 
    tạo ra ID số 1, thì khi gộp lại sẽ xảy ra "Xung đột định danh".
    
    File `oid_manager.py` này giải quyết vấn đề đó bằng cách tạo ra một định dạng ID Mới gọi là OID 
    (Object Identifier). Nó đảm bảo OID là DUY NHẤT TRÊN TOÀN BỘ MẠNG LƯỚI toàn cầu, bất kể đối tượng
    đó được sinh ra ở máy chủ nào.

Dựa trên lý thuyết:
    Özsu & Valduriez "Principles of Distributed Database Systems" (4th Ed.)
    - Chương 12: Object-Oriented Distributed Databases
    - Mục 12.2: Object Identity in Distributed Systems (Vấn đề khủng hoảng danh tính và cách giải quyết)
"""

import threading # Thư viện để khóa (Lock) luồng, chống lỗi khi có nhiều request tới cùng 1 lúc (Race condition)
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, Optional


# =============================================================================
# LỚP CẤU TRÚC ĐỊNH DANH (OID CLASS)
# =============================================================================

@dataclass
class OID:
    """
    Đại diện cho Định danh Đối tượng (OID - Object ID) của hệ thống.
    
    Cách thiết kế (Mục 12.2.3 - Structured OIDs):
    Thay vì dùng một chuỗi UUID ngẫu nhiên dài thòng (vừa tốn dung lượng vừa khó đọc), 
    hệ thống dùng "OID có cấu trúc" gồm 3 phần:
        <site_id>.<class_id>.<sequence_number>
        Ví dụ: 0.Vehicle.00001
        
    Ý nghĩa:
        0         : Máy chủ sinh ra nó (Site 0)
        Vehicle   : Tên Lớp sinh ra nó
        00001     : Số thứ tự tăng dần ở máy chủ đó
        
    Lợi ích: Chỉ cần nhìn vào OID là Điều phối viên (Coordinator) lập tức biết phải 
    chạy tới máy chủ nào để lấy thông tin gốc (Định tuyến truy vấn siêu tốc).
    """
    site_id: int        # ID của Site (0, 1, 2)
    class_name: str     # Tên của Lớp (Vehicle, Truck, ElectricCar)
    sequence: int       # Số thứ tự (Bộ đếm tăng dần)

    def __str__(self) -> str:
        """Hàm tự động được gọi khi dùng lệnh print() hoặc ép kiểu str(). Giúp format số thứ tự thành 5 chữ số."""
        return f"{self.site_id}.{self.class_name}.{self.sequence:05d}"

    def __repr__(self) -> str:
        """Hàm dùng cho log debug để hiển thị rõ đây là object OID."""
        return f"OID({self!s})"

    def __hash__(self):
        """Hàm để OID có thể làm Key (Khóa) trong các kiểu dữ liệu Dictionary/Set của Python."""
        return hash(str(self))

    def __eq__(self, other):
        """Hàm so sánh (Dấu ==). Trả về True nếu 2 OID có cùng Site, cùng Tên lớp và cùng Số thứ tự."""
        if isinstance(other, OID):
            return (self.site_id == other.site_id and
                    self.class_name == other.class_name and
                    self.sequence == other.sequence)
        return False

    @classmethod
    def from_string(cls, oid_str: str) -> "OID":
        """
        Hàm tiện ích (Factory Method). 
        Nhận vào 1 chuỗi String (VD: "0.Vehicle.00001") và bẻ nó ra làm 3 mảnh để ráp lại thành 1 đối tượng OID.
        """
        parts = oid_str.split(".")
        if len(parts) != 3:
            raise ValueError(f"Định dạng OID không hợp lệ: {oid_str!r}. Định dạng mong đợi 'site.class.seq'")
        try:
            site_id = int(parts[0])      # Mảnh 1 (ID Site)
            class_name = parts[1]        # Mảnh 2 (Tên class)
            sequence = int(parts[2])     # Mảnh 3 (Số thứ tự)
            return cls(site_id=site_id, class_name=class_name, sequence=sequence)
        except (ValueError, IndexError) as exc:
            raise ValueError(f"Không thể phân tích OID '{oid_str}': {exc}") from exc

    def home_site(self) -> int:
        """Trả về ID của site nơi đối tượng này được sinh ra (Home Site)."""
        return self.site_id


# =============================================================================
# BỘ QUẢN LÝ OID (OID MANAGER)
# =============================================================================

class OIDManager:
    """
    Mỗi Site sẽ sở hữu riêng 1 cuốn "Sổ đăng ký OID" (OIDManager).
    
    Nhiệm vụ: Cấp phát số thứ tự (Sequence) mới khi có xe mới được Insert vào hệ thống,
    đảm bảo không bao giờ có 2 chiếc xe bị trùng số.
    """

    def __init__(self, site_id: int):
        self.site_id = site_id
        
        # Biến đếm: Lưu trữ số thứ tự lớn nhất hiện tại của từng Lớp. (VD: {"Vehicle": 15})
        self._counters: Dict[str, int] = {} 
        
        # Ổ khóa (Lock): Chặn các luồng (Thread) đụng nhau. 
        # Ví dụ: Có 2 Request cùng gửi tới Insert xe lúc 12:00:00, ổ khóa sẽ bắt 1 thằng phải xếp hàng
        # chờ thằng kia lấy số 16 xong, thì thằng thứ hai mới được lấy số 17. (Chống Race condition).
        self._lock = threading.Lock()
        
        # Cuốn sổ cái: Lưu lại lịch sử toàn bộ OID đã sinh ra để tra cứu nhanh.
        self._registry: Dict[str, OID] = {}   

    def generate(self, class_name: str) -> OID:
        """
        Hàm bấm số (Cấp phát OID mới) cho đối tượng thuộc lớp `class_name`.
        """
        # Khóa cửa lại (Chỉ cho phép 1 luồng chạy vào đây tại 1 thời điểm)
        with self._lock:
            # Nếu lớp này chưa bao giờ cấp số (chưa có trong _counters), thì gán nó bằng 0
            self._counters.setdefault(class_name, 0)
            
            # Tăng số thứ tự lên 1 (VD: 15 -> 16)
            self._counters[class_name] += 1
            
            # Khởi tạo đối tượng OID mới
            oid = OID(
                site_id=self.site_id,
                class_name=class_name,
                sequence=self._counters[class_name],
            )
            
            # Ghi vào cuốn sổ cái
            self._registry[str(oid)] = oid
            return oid

    def lookup(self, oid_str: str) -> Optional[OID]:
        """
        Hàm tra cứu nhanh (Mở sổ cái tìm OID).
        Nếu tìm thấy trả về đối tượng OID, nếu không có trả về None (Không tìm thấy).
        """
        return self._registry.get(oid_str)

    def stats(self) -> Dict[str, int]:
        """
        Hàm thống kê: Trả về trạng thái của bộ đếm để báo cáo ra Dashboard / Terminal.
        (Được dùng bởi Menu [7] Khảo sát Hệ thống).
        """
        # Khóa lại trong lúc lấy dữ liệu để tránh dữ liệu bị sai lệch
        with self._lock:
            return dict(self._counters)
