"""
Máy chủ Site (Site Server / Worker Node) - Nơi chứa dữ liệu vật lý
==================================================================
Vai trò của file:
    File `site_server.py` đóng vai trò là "Nhân viên" (Worker Node). Mỗi Site (0, 1, 2) sẽ 
    chạy một bản sao của file này. Nhiệm vụ của nó là lắng nghe các truy vấn HTTP từ Coordinator, 
    nhúng tay vào PostgreSQL để Lấy/Ghi dữ liệu, rồi gói vào JSON trả về.

    Nó cung cấp các API (RESTful):
    - GET  /ping                 : Trả lời "Tôi còn sống".
    - POST /insert               : Ghi dữ liệu vào đĩa cứng (PostgreSQL).
    - GET  /query                : Tìm kiếm dữ liệu trong máy này.
    - POST /schema_evolve        : Cập nhật thêm thuộc tính mới vào tất cả xe trong máy này.

Dựa trên lý thuyết:
    Özsu & Valduriez "Principles of Distributed Database Systems" (4th Ed.)
    - Chương 3: Kiến trúc DBMS phân tán.
    - Mục 10.1: Giao tiếp giữa các site (Inter-site communication).
"""

import json
import os
import sys
import time
from typing import Any, Dict, List, Optional
import psycopg2          # Thư viện để kết nối và tương tác với PostgreSQL

from flask import Flask, jsonify, request # Flask là Framework để tạo Web Server / API Server

# Đảm bảo Python hiểu được đường dẫn để import các file khác trong thư mục src/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.config import SITES, CLASS_HIERARCHY, SCHEMA_VERSION
from src.oid_manager import OID, OIDManager
from src.models import Vehicle, Truck, ElectricCar, deserialize_object, serialize_object, CLASS_MAP


# =============================================================================
# LỚP MÁY CHỦ SITE (SITE SERVER CLASS)
# =============================================================================

class SiteServer:
    """
    Mỗi đối tượng SiteServer đại diện cho một Máy chủ Độc lập.
    Tính tự trị cục bộ (Local Autonomy): Nó tự có CSDL riêng, tự quản lý OID của riêng nó.
    """

    def __init__(self, site_id: int):
        self.site_id = site_id                           # ID của site (0, 1 hoặc 2)
        self.site_config = SITES[site_id]                # Lấy cấu hình từ config.py
        self.class_name = self.site_config["class"]      # Lấy tên Class mà site này phải giữ (VD: "Truck")
        self.db_uri = self.site_config.get("db_uri")     # Chuỗi kết nối tới PostgreSQL
        
        self.oid_manager = OIDManager(site_id)           # Khởi tạo cuốn sổ đăng ký OID cho Site này
        self.objects: Dict[str, Dict[str, Any]] = {}     # Bộ đệm (Cache) trên RAM (Tốc độ đọc siêu nhanh)
        
        # Kết nối CSDL Postgres và tạo bảng (nếu chưa có)
        self._init_db()
        self._load_from_db()
        
        # Theo dõi lịch sử tiến hóa lược đồ (Để biết CSDL đã được thêm cột gì, lúc nào)
        self._schema_history: List[Dict] = []


        # Khởi tạo Web Server (Flask)
        self.app = Flask(f"site_{site_id}")
        self._register_routes()

    # ------------------------------------------------------------------
    # CÁC HÀM TƯƠNG TÁC VỚI Ổ ĐĨA VẬT LÝ (POSTGRESQL)
    # ------------------------------------------------------------------

    def _init_db(self) -> None:
        """Tạo bảng PostgreSQL tên là `objects`. Bảng này chỉ có đúng 2 cột: OID và Cục JSONB."""
        try:
            # Mở kết nối
            with psycopg2.connect(self.db_uri) as conn:
                with conn.cursor() as cur:
                    # Tạo bảng bằng ngôn ngữ SQL
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS objects (
                            oid VARCHAR(255) PRIMARY KEY,
                            data JSONB NOT NULL
                        )
                    """)
                conn.commit() # Lưu thay đổi
            print(f"[Site {self.site_id}] Đã khởi tạo bảng PostgreSQL thành công.")
        except Exception as exc:
            print(f"[Site {self.site_id}] LỖI khởi tạo DB: {exc}")

    def _load_from_db(self) -> None:
        """Khi Server vừa bật lên, phải móc dữ liệu từ ổ cứng (PostgreSQL) nạp lên RAM."""
        try:
            with psycopg2.connect(self.db_uri) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT oid, data FROM objects")
                    rows = cur.fetchall()
                    for r in rows:
                        oid_str, raw_data = r[0], r[1]
                        
                        # Nạp vào RAM
                        self.objects[oid_str] = raw_data
                        
                        # Báo lại cho OID Manager biết để nó cập nhật Số thứ tự lớn nhất, tránh cấp trùng ID
                        try:
                            oid = OID.from_string(oid_str)
                            cls_name = oid.class_name
                            self.oid_manager._counters.setdefault(cls_name, 0)
                            if oid.sequence > self.oid_manager._counters[cls_name]:
                                self.oid_manager._counters[cls_name] = oid.sequence
                            self.oid_manager._registry[oid_str] = oid
                        except ValueError:
                            pass
        except Exception as exc:
            print(f"[Site {self.site_id}] LỖI tải dữ liệu DB: {exc}")
            self.objects = {}

    def _save_to_db(self) -> None:
        """Hàm lưu toàn bộ RAM xuống ổ cứng. Có dùng cơ chế UPSERT (Nếu trùng OID thì đè lên)."""
        try:
            with psycopg2.connect(self.db_uri) as conn:
                with conn.cursor() as cur:
                    for oid_str, obj_data in self.objects.items():
                        cur.execute("""
                            INSERT INTO objects (oid, data) 
                            VALUES (%s, %s)
                            ON CONFLICT (oid) DO UPDATE SET data = EXCLUDED.data
                        """, (oid_str, json.dumps(obj_data)))
                conn.commit()
        except Exception as exc:
            print(f"[Site {self.site_id}] LỖI lưu dữ liệu DB: {exc}")

    # ------------------------------------------------------------------
    # CÁC API HTTP (ENDPOINTS) CỦA WORKER NODE
    # ------------------------------------------------------------------

    def _register_routes(self) -> None:
        """Khai báo các đường dẫn API mà máy chủ này có thể nhận."""
        app = self.app

        @app.route("/ping", methods=["GET"])
        def ping():
            """Hành động: Trả lời Coordinator rằng 'Tôi còn sống'."""
            return jsonify({
                "status": "ok",
                "site_id": self.site_id,
                "site_name": self.site_config["name"],
                "class": self.class_name,
                "object_count": len(self.objects),
                "timestamp": time.time(),
            })

        @app.route("/schema", methods=["GET"])
        def schema():
            """Trả về cấu trúc Lược đồ hiện tại của Site."""
            hier = CLASS_HIERARCHY.get(self.class_name, {})
            return jsonify({
                "site_id": self.site_id,
                "class": self.class_name,
                "parent": hier.get("parent"),
                "attributes": hier.get("attributes", []),
                "schema_version": SCHEMA_VERSION,
                "schema_history": self._schema_history,
            })

        @app.route("/insert", methods=["POST"])
        def insert():
            """
            Hành động: Nhận dữ liệu từ Coordinator và ghi vào đĩa cứng.
            Mấu chốt của Phục hồi (Rehydration): 
              - Nếu Site 1 (Truck) nhận được request cấp OID=0.Vehicle.001 từ Coordinator, 
                nó KHÔNG sinh OID mới, mà nó DÙNG LUÔN OID đó. 
              - Cùng 1 OID chia cho 2 mảnh, sau này lấy OID đó làm chìa khóa ráp 2 mảnh lại.
            """
            data = request.get_json(force=True)
            if not data:
                return jsonify({"error": "Không có thân JSON nào được cung cấp"}), 400

            # Lấy OID do Coordinator cung cấp (Nếu có)
            existing_oid_str = data.get("oid")
            if existing_oid_str:
                try:
                    new_oid = OID.from_string(existing_oid_str)
                    self.oid_manager._registry[existing_oid_str] = new_oid
                    cls_key = new_oid.class_name
                    self.oid_manager._counters.setdefault(cls_key, 0)
                    if new_oid.sequence > self.oid_manager._counters[cls_key]:
                        self.oid_manager._counters[cls_key] = new_oid.sequence
                except ValueError:
                    new_oid = self.oid_manager.generate(self.class_name)
            else:
                # Nếu không cung cấp, tự sinh OID mới
                new_oid = self.oid_manager.generate(self.class_name)

            # Gán thông tin chuẩn vào JSON trước khi ghi đĩa
            data["oid"] = str(new_oid)
            data["__class__"] = data.get("__class__", self.class_name)
            data.setdefault("schema_version", SCHEMA_VERSION)

            try:
                obj = deserialize_object(data)
            except Exception as exc:
                return jsonify({"error": f"Quá trình phục hồi (deserialization) thất bại: {exc}"}), 422

            # Ghi vào RAM và ép xuống Đĩa
            self.objects[str(new_oid)] = serialize_object(obj)
            self._save_to_db()
            return jsonify({"oid": str(new_oid), "status": "inserted"}), 201

        @app.route("/objects", methods=["GET"])
        def get_all_objects():
            """Trả về toàn bộ dữ liệu có trong máy này (Không có bộ lọc)."""
            limit = request.args.get("limit", type=int)
            objects_list = list(self.objects.values())
            if limit and limit > 0:
                objects_list = objects_list[:limit]
                
            return jsonify({
                "site_id": self.site_id,
                "class": self.class_name,
                "objects": objects_list,
                "count": len(objects_list),
            })

        @app.route("/object/<path:oid_str>", methods=["GET"])
        def get_object(oid_str: str):
            """Lấy 1 chiếc xe cụ thể dựa trên OID của nó."""
            obj_data = self.objects.get(oid_str)
            if obj_data is None:
                return jsonify({"error": f"Không tìm thấy OID '{oid_str}' tại site {self.site_id}"}), 404
            return jsonify(obj_data)

        @app.route("/query", methods=["GET"])
        def query():
            """
            Lọc (Filter) tìm kiếm xe. 
            Mô phỏng "Mệnh đề WHERE" (Selection) trong CSDL Quan hệ.
            """
            # Đọc các tham số tìm kiếm từ URL
            field = request.args.get("field")
            value = request.args.get("value")
            year_min = request.args.get("year_min", type=int)
            year_max = request.args.get("year_max", type=int)

            results = []
            # Quét vòng lặp toàn bộ dữ liệu trên RAM để tìm
            for obj_data in self.objects.values():
                match = True
                if field and value:
                    obj_val = str(obj_data.get(field, ""))
                    if obj_val.lower() != value.lower():
                        match = False
                if year_min and obj_data.get("year", 0) < year_min:
                    match = False
                if year_max and obj_data.get("year", 0) > year_max:
                    match = False
                
                # Nếu thỏa mãn mọi điều kiện thì nhét vào mảng kết quả
                if match:
                    results.append(obj_data)
                    
            limit = request.args.get("limit", type=int)
            if limit and limit > 0:
                results = results[:limit]

            return jsonify({
                "site_id": self.site_id,
                "class": self.class_name,
                "results": results,
                "count": len(results),
            })

        @app.route("/schema_evolve", methods=["POST"])
        def schema_evolve():
            """
            Tiến hóa Lược đồ tại Site cục bộ.
            Hành động: Quét toàn bộ xe đang có trong máy, Dán thêm (Add) cái thuộc tính mới vào.
            """
            data = request.get_json(force=True)
            attr_name = data.get("attribute")       # Tên cột mới (VD: fuel_type)
            default_value = data.get("default")     # Giá trị mặc định (VD: Diesel)
            new_version = data.get("new_version", SCHEMA_VERSION) # VD: v1.1.0

            if not attr_name:
                return jsonify({"error": "Thiếu trường 'attribute'"}), 400

            updated = 0
            # Quét và dán
            for oid_str, obj_data in self.objects.items():
                if attr_name not in obj_data:
                    obj_data[attr_name] = default_value
                    obj_data["schema_version"] = new_version
                    updated += 1

            # Ghi lại lịch sử
            self._schema_history.append({
                "action": "add_attribute",
                "attribute": attr_name,
                "default": default_value,
                "new_version": new_version,
                "objects_updated": updated,
                "timestamp": time.time(),
            })
            
            # Đè toàn bộ RAM xuống Ổ cứng (Postgres)
            self._save_to_db()
            
            return jsonify({
                "site_id": self.site_id,
                "attribute_added": attr_name,
                "objects_updated": updated,
                "new_schema_version": new_version,
            })



        @app.route("/stats", methods=["GET"])
        def stats():
            """Hàm phục vụ báo cáo hệ thống."""
            return jsonify({
                "site_id": self.site_id,
                "oid_stats": self.oid_manager.stats(),
                "object_count": len(self.objects),
                "schema_version": SCHEMA_VERSION,
            })

        # =================================================================
        # CÁC API CHỈ DÀNH RIÊNG CHO ĐIỀU PHỐI VIÊN (MASTER NODE)
        # =================================================================
        # Code này chỉ được kích hoạt nếu Site đó là Site 0.
        if self.site_id == 0:
            from src.coordinator import Coordinator
            self.coordinator_engine = Coordinator() # Mời Giám đốc (Coordinator) vào làm việc

            @app.route("/global/search", methods=["GET"])
            def global_search():
                """Khi Client (main.py) gọi hàm tìm kiếm, nó sẽ vào đây."""
                field = request.args.get("field")
                value = request.args.get("value")
                year_min = request.args.get("year_min", type=int)
                year_max = request.args.get("year_max", type=int)
                sites_arg = request.args.get("include_sites")
                
                include_sites = None
                if sites_arg:
                    include_sites = [int(s) for s in sites_arg.split(",")]

                # Chuyển lệnh tìm kiếm này cho Coordinator giải quyết (Xem Coordinator_engine.py)
                result = self.coordinator_engine.polymorphic_search(
                    field=field, value=value, year_min=year_min, year_max=year_max, include_sites=include_sites
                )
                
                return jsonify({
                    "objects": [serialize_object(obj) for obj in result.objects],
                    "timing": result.timing,
                    "errors": result.errors,
                    "rehydration_count": result.rehydration_count,
                    "total_time": result.total_time,
                    "summary_text": result.summary()
                })

            @app.route("/global/schema_evolve", methods=["POST"])
            def global_schema_evolve():
                data = request.get_json(force=True)
                results = self.coordinator_engine.schema_evolve(
                    attribute=data["attribute"],
                    default_value=data["default"],
                    new_version=data["new_version"]
                )
                return jsonify(results)

            @app.route("/global/stats", methods=["GET"])
            def global_stats():
                return jsonify(self.coordinator_engine.get_site_stats())

    def run(self) -> None:
        """Hàm bật máy chủ Flask lắng nghe Request."""
        host = self.site_config.get("flask_host", self.site_config["host"])
        port = self.site_config["port"]
        print(f"[Site {self.site_id}] Đang khởi động {self.site_config['name']} trên {host}:{port}")
        self.app.run(host=host, port=port, debug=False, use_reloader=False)


# =============================================================================
# ĐIỂM KÍCH HOẠT KHI CHẠY DÒNG LỆNH
# =============================================================================

if __name__ == "__main__":
    import argparse

    # Đọc tham số từ dòng lệnh (Ví dụ: python site_server.py 1)
    parser = argparse.ArgumentParser(description="Khởi chạy một máy chủ phân tán Vehicle Fleet")
    parser.add_argument("site_id", type=int, choices=[0, 1, 2],
                        help="Site ID để khởi chạy (0=Vehicle, 1=Truck, 2=ElectricCar)")
    args = parser.parse_args()

    # Bật Server
    server = SiteServer(args.site_id)
    server.run()
