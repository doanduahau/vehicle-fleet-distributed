"""
Điều phối viên (Coordinator) - Bộ Não của Hệ thống CSDL Phân tán
================================================================
Vai trò của file:
    File `coordinator.py` này đóng vai trò như một "Giám đốc điều hành" (Coordinator Node / Master Node).
    Trong kiến trúc Master-Worker, nó là bộ phận duy nhất giao tiếp với Client (người dùng).
    Nó nhận lệnh từ Client, sau đó "chỉ tay năm ngón", phân phát công việc xuống cho các "nhân viên" 
    (Worker Nodes - Site 1, Site 2) để lấy dữ liệu. Cuối cùng, nó thu thập, nhào nặn dữ liệu 
    thành một cục hoàn chỉnh rồi gửi trả lại cho Client.

Các nhiệm vụ cốt lõi:
    1. Tìm kiếm Đa hình (Polymorphic Search): Phân tán câu truy vấn ra các Site.
    2. Phục hồi đối tượng (Rehydration): Ghép mảnh Khung gầm ở Site 0 với mảnh Động cơ ở Site 1.
    3. Nhận thức mạng (Network Awareness): Bắt lỗi Timeout, ghi log chi phí mạng.
    4. Tiến hóa Lược đồ (Schema Evolution): Phát lệnh thêm cột dữ liệu tới toàn mạng.

Dựa trên lý thuyết:
    - Xử lý truy vấn phân tán (Distributed Query Processing) - Cơ chế Fan-out (Tỏa nhánh) và Join.
    - Phục hồi đối tượng (Object Rehydration) trong cơ sở dữ liệu hướng đối tượng phân tán.
"""

import concurrent.futures # Thư viện Xử lý đa luồng (Multi-threading) - Quan trọng để gọi các Site cùng lúc
import json               # Thư viện xử lý JSON
import sys
import time               # Thư viện đo lường thời gian mạng
from typing import Any, Dict, List, Optional, Tuple

import requests           # Thư viện HTTP Client để gọi API xuyên mạng Docker

# Khắc phục lỗi hiển thị tiếng Việt trên Terminal Windows
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except AttributeError:
    pass

from src.config import SITES, COORDINATOR_SITE
from src.models import deserialize_object, Vehicle


# =============================================================================
# LỚP BÁO CÁO KẾT QUẢ TRUY VẤN (QUERY RESULT CONTAINER)
# =============================================================================

class QueryResult:
    """
    Một chiếc hộp để chứa kết quả sau khi Điều phối viên lấy dữ liệu xong.
    Nó không chỉ chứa danh sách Xe (objects), mà còn chứa hóa đơn thanh toán thời gian (timing) 
    để chứng minh cho giáo viên thấy hệ thống đã tốn bao nhiêu mili-giây để truyền qua mạng.
    """

    def __init__(self):
        self.objects: List[Vehicle] = []             # Danh sách các đối tượng Xe đã được ghép nối
        self.timing: Dict[str, float] = {}           # site_id -> Số giây tốn để lấy dữ liệu
        self.errors: Dict[int, str] = {}             # site_id -> Câu thông báo lỗi (Nếu Site bị sập)
        self.rehydration_count: int = 0              # Đếm số lần phải khâu/ghép (Join) 2 nửa đối tượng
        self.rehydration_by_site: Dict[int, int] = {1: 0, 2: 0} # Phục hồi theo từng site
        self.total_time: float = 0.0                 # Tổng thời gian từ lúc bấm tới lúc hiện ra

    def summary(self) -> str:
        """Hàm định dạng bảng báo cáo để in ra màn hình Client."""
        lines = [
            "=" * 60,
            "TÓM TẮT KẾT QUẢ TÌM KIẾM ĐA HÌNH (POLYMORPHIC SEARCH)",
            "=" * 60,
            f"  Tổng số đối tượng tìm thấy : {len(self.objects)}",
            f"  Số đối tượng được phục hồi : {self.rehydration_count} (cần kết hợp xuyên site)",
            "  Phục hồi theo từng site:",
        ]
        for site_id, count in self.rehydration_by_site.items():
            name = SITES.get(site_id, {}).get("name", f"Site {site_id}")
            lines.append(f"    Site-{site_id} ({name}): {count} đối tượng")
            
        lines.extend([
            f"  Tổng thời gian (wall-clock) : {self.total_time:.3f}s",
            "",
            "  Thời gian trễ mạng (Network Latency) từng site:",
        ])
            
        # In ra thời gian trễ của từng Site
        for site_id, t in self.timing.items():
            name = SITES.get(int(site_id), {}).get("name", f"Site {site_id}")
            lines.append(f"    Site-{site_id} ({name}): {t:.3f}s")
            
        # In ra màu đỏ nếu có Site nào bị rớt mạng (Graceful Degradation)
        if self.errors:
            lines.append("")
            lines.append("  LỖI (các site không thể truy cập nhưng hệ thống VẪN CHẠY):")
            for site_id, err in self.errors.items():
                lines.append(f"    Site {site_id}: {err}")
        lines.append("=" * 60)
        return "\n".join(lines)


# =============================================================================
# LỚP ĐIỀU PHỐI VIÊN (COORDINATOR ENGINE)
# =============================================================================

class Coordinator:
    """
    Trái tim của hệ thống phân tán.
    Lớp này tạo ra các Request HTTP để kết nối các mảnh vỡ (Fragments) lại với nhau.
    """

    def __init__(self):
        self.coordinator_site = COORDINATOR_SITE
        self.sites = SITES
        
        # Tạo một Session HTTP. (Giúp dùng lại kết nối mạng (Keep-Alive) để tối ưu tốc độ)
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

        # =================================================================
        # DISTRIBUTED SCHEMA REGISTRY (HỆ QUẢN TRỊ LƯỢC ĐỒ PHÂN TÁN)
        # =================================================================
        import os
        os.makedirs("data", exist_ok=True)
        self._registry_file = "data/schema_registry.json"
        self._load_schema_registry()

    def _load_schema_registry(self):
        import os
        if os.path.exists(self._registry_file):
            try:
                with open(self._registry_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.global_schema_version = data.get("version", "1.0.0")
                    self.schema_changelog = data.get("changelog", [])
                    # json keys are strings, convert back to int for sites
                    pending = data.get("pending", {})
                    self.pending_schemas = {int(k): v for k, v in pending.items()}
            except Exception as e:
                print(f"[Schema Registry] Lỗi nạp sổ cái từ ổ đĩa: {e}")
                self._init_default_registry()
        else:
            self._init_default_registry()

    def _init_default_registry(self):
        self.global_schema_version = "1.0.0"
        self.schema_changelog = []
        self.pending_schemas = {site_id: [] for site_id in self.sites}
        
    def _save_schema_registry(self):
        try:
            with open(self._registry_file, "w", encoding="utf-8") as f:
                json.dump({
                    "version": self.global_schema_version,
                    "changelog": self.schema_changelog,
                    "pending": self.pending_schemas
                }, f, indent=4)
        except Exception as e:
            print(f"[Schema Registry] Lỗi lưu sổ cái xuống ổ đĩa: {e}")

    def _site_url(self, site_id: int, path: str) -> str:
        """Hàm tiện ích ghép Host và Port thành URL đầy đủ (Ví dụ: http://site1:5001/query)"""
        cfg = self.sites[site_id]
        return f"http://{cfg['host']}:{cfg['port']}{path}"

    def _ping_site(self, site_id: int) -> Tuple[bool, str]:
        """Hàm gõ cửa (Ping) một site xem nó còn thức hay đã sập."""
        try:
            url = self._site_url(site_id, "/ping")
            resp = self._session.get(url, timeout=2.0)
            data = resp.json()
            return True, data.get("site_name", f"Site {site_id}")
        except Exception as exc:
            return False, str(exc)

    def check_all_sites(self) -> Dict[int, bool]:
        """Gõ cửa toàn bộ mạng lưới để làm báo cáo thống kê."""
        status = {}
        for site_id in self.sites:
            alive, info = self._ping_site(site_id)
            status[site_id] = alive
            status_str = "[ONLINE] " if alive else "[OFFLINE]"
            print(f"  {status_str} {self.sites[site_id]['name']}: {info}")
        return status

    # -------------------------------------------------------------------------
    # HÀM LẤY DỮ LIỆU TỪ 1 SITE BẤT KỲ (WORKER THREAD)
    # -------------------------------------------------------------------------
    def _fetch_from_site(
        self,
        site_id: int,
        class_name: Optional[str] = None,
        field: Optional[str] = None,
        value: Optional[str] = None,
        op: Optional[str] = None,
        year_min: Optional[int] = None,
        year_max: Optional[int] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        oids: Optional[List[str]] = None,
    ) -> Tuple[int, List[Dict], float, Optional[str]]:
        """
        Hành động: Gửi mạng HTTP tới 1 máy chủ cụ thể để lấy dữ liệu.
        Rất quan trọng: Có cơ chế Bắt Lỗi Mạng (Network Timeout) cực kỳ tinh vi.
        """
        t0 = time.perf_counter() # Bấm đồng hồ bắt đầu
        
        site_timeout = self.sites[site_id].get("timeout", 2.0)
        
        try:
            # Gói ghém các điều kiện tìm kiếm vào URL Params
            # Lưu ý: Không truyền class_name xuống Site để tránh Site lọc sai. 
            # Việc lọc class_name sẽ được Coordinator thực hiện ở bước Final Filter.
            params = {}
            if field: params["field"] = field
            if value is not None: params["value"] = value
            if op: params["op"] = op
            if year_min: params["year_min"] = year_min
            if year_max: params["year_max"] = year_max
            if limit: params["limit"] = limit
            if offset: params["offset"] = offset

            if oids is not None:
                url = self._site_url(site_id, "/query")
                print(f"  [NETWORK LOG] Gửi POST request tới Site {site_id} ({url}) với {len(oids)} OIDs (Semi-Join)")
                resp = self._session.post(url, json={"oids": oids}, params=params, timeout=site_timeout)
            else:
                url = self._site_url(site_id, "/query") if params else self._site_url(site_id, "/objects")
                print(f"  [NETWORK LOG] Gửi GET request tới Site {site_id} ({url}) với TTL={site_timeout}s")
                resp = self._session.get(url, params=params, timeout=site_timeout)
            
            data = resp.json()
            
            elapsed = time.perf_counter() - t0 # Dừng đồng hồ
            objects = data.get("objects", data.get("results", []))
            
            print(f"  [NETWORK LOG] Nhận response từ Site {site_id}: {len(objects)} đối tượng trong {elapsed*1000:.1f}ms")
            return site_id, objects, elapsed, None
            
        except requests.exceptions.ConnectionError:
            # Lỗi: Không thể kết nối (Do container của Site đó bị Stop)
            elapsed = time.perf_counter() - t0
            print(f"  [NETWORK LOG] KẾT NỐI THẤT BẠI tới Site {site_id} (Connection Refused)")
            return site_id, [], elapsed, f"Từ chối kết nối (site có thể đang bảo trì/offline)"
            
        except requests.exceptions.Timeout:
            # Lỗi: Site có chạy nhưng mạng quá chậm hoặc bị quá tải -> Chém đứt kết nối (Cắt đuôi)
            elapsed = time.perf_counter() - t0
            print(f"  [NETWORK LOG] TIMEOUT tại Site {site_id} (vượt quá {site_timeout}s)")
            return site_id, [], elapsed, f"Hết thời gian chờ (Timeout) sau {site_timeout}s"
            
        except Exception as exc:
            # Các lỗi rác khác
            elapsed = time.perf_counter() - t0
            print(f"  [NETWORK LOG] LỖI KHÔNG XÁC ĐỊNH tại Site {site_id}: {exc}")
            return site_id, [], elapsed, str(exc)

    def choose_driver_site(self, class_name: Optional[str], field: Optional[str]) -> int:
        from src.config import ATTRIBUTE_SITE, CLASS_SITE
        
        if field in ATTRIBUTE_SITE:
            return ATTRIBUTE_SITE[field]
        if class_name and class_name != "Vehicle" and class_name in CLASS_SITE:
            return CLASS_SITE[class_name]
        return 0

    def polymorphic_search(
        self,
        class_name: Optional[str] = None,
        field: Optional[str] = None,
        value: Optional[str] = None,
        op: Optional[str] = None,
        year_min: Optional[int] = None,
        year_max: Optional[int] = None,
        limit: Optional[int] = None,
        include_sites: Optional[List[int]] = None,
    ) -> QueryResult:
        """
        Nhiệm vụ: Cùng một lúc gọi tới cả 3 máy chủ, lượm lặt dữ liệu, khâu chúng lại, trả về danh sách cuối.
        """
        # Đồng bộ Schema Eventual Consistency trước khi query
        self.sync_pending_schemas()

        t_start = time.perf_counter()
        result = QueryResult()
        
        # Nếu không truyền vào mảng các site cần query, mặc định query tât cả các site
        sites_to_query = include_sites if include_sites is not None else list(self.sites.keys())

        print(f"\n[Coordinator] Đang bắt đầu Tìm kiếm Đa hình (Polymorphic Search)")
        print(f"  Bộ lọc: class={class_name!r}, field={field!r}, op={op!r}, value={value!r}, year_min={year_min}, year_max={year_max}")
        print(f"  Các Site truy vấn: {sites_to_query}")

        site_data: Dict[int, List[Dict]] = {}
        driver_site = self.choose_driver_site(class_name, field)
        
        if limit and driver_site in sites_to_query:
            print(f"  Chiến lược: Bán kết nối (Semi-Join) phân lô (Batching) do có LIMIT={limit}. Driver Site: Site {driver_site}\n")
            
            collected_objects = []
            offset = 0
            # Lấy dư ra một chút (batch_size) để bù hao hụt sau khi filter
            batch_size = limit * 3 if (class_name and driver_site == 0) else limit
            
            while len(collected_objects) < limit:
                # BƯỚC 1: Lấy dữ liệu từ Driver Site
                sid, objects, elapsed, error = self._fetch_from_site(driver_site, class_name, field, value, op, year_min, year_max, limit=batch_size, offset=offset)
                result.timing[driver_site] = result.timing.get(driver_site, 0.0) + elapsed
                
                if error:
                    result.errors[driver_site] = error
                    print(f"  [WARN] Driver Site {driver_site}: {error}")
                    break
                    
                if not objects:
                    break # Hết dữ liệu ở Driver Site
                    
                site_data_batch = {driver_site: objects}
                target_oids = [obj.get("oid") for obj in objects if "oid" in obj]
                print(f"  [OK]   Driver Site {driver_site}: Tìm thấy {len(target_oids)} OIDs gốc [{elapsed:.3f}s] (Offset: {offset})")
                
                # BƯỚC 2: Fetch song song từ các site còn lại
                other_sites = [s for s in sites_to_query if s != driver_site]
                if target_oids and other_sites:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=len(other_sites)) as executor:
                        futures = {
                            executor.submit(self._fetch_from_site, s, None, None, None, None, None, None, None, None, target_oids): s
                            for s in other_sites
                        }
                        for future in concurrent.futures.as_completed(futures):
                            s, obj_list, elap, err = future.result()
                            result.timing[s] = result.timing.get(s, 0.0) + elap
                            if err:
                                result.errors[s] = err
                                print(f"  [WARN] Site {s}: {err} [{elap:.3f}s]")
                            else:
                                site_data_batch[s] = obj_list
                                print(f"  [OK]   Site {s}: Lấy {len(obj_list)} mảnh mở rộng [{elap:.3f}s]")
                
                # BƯỚC 3: Rehydrate (Khâu các mảnh lại với nhau) cho batch hiện tại
                subclass_by_oid = {}
                for s in [1, 2]:
                    for obj_data in site_data_batch.get(s, []):
                        oid_str = obj_data.get("oid", "")
                        if oid_str: subclass_by_oid[oid_str] = (s, obj_data)

                batch_rehydrated = []
                for base_data in site_data_batch.get(0, []):
                    oid_str = base_data.get("oid", "")
                    if oid_str in subclass_by_oid:
                        source_site, subclass_data = subclass_by_oid[oid_str]
                        merged = {**base_data, **subclass_data}
                    else:
                        merged = base_data

                    try:
                        obj = deserialize_object(merged)
                        batch_rehydrated.append(obj)
                    except Exception:
                        pass

                base_oids = {d.get("oid") for d in site_data_batch.get(0, [])}
                for s in [1, 2]:
                    for obj_data in site_data_batch.get(s, []):
                        oid_str = obj_data.get("oid", "")
                        if oid_str not in base_oids:
                            try:
                                obj = deserialize_object(obj_data)
                                batch_rehydrated.append(obj)
                            except Exception:
                                pass
                                
                # BƯỚC 4: Lọc Final Class cho batch
                if class_name and class_name != "Vehicle":
                    batch_rehydrated = [obj for obj in batch_rehydrated if obj.__class__.__name__ == class_name]
                    
                collected_objects.extend(batch_rehydrated)
                print(f"  [BATCH] Đã tích lũy được {len(collected_objects)}/{limit} đối tượng thỏa mãn...")
                
                offset += batch_size
                if len(objects) < batch_size:
                    break # Không còn dữ liệu ở site gốc để lấy thêm
                    
            result.objects = collected_objects[:limit]
            
            # Tính toán lại thông số phục hồi chuẩn xác cho kết quả cuối
            result.rehydration_count = sum(1 for obj in result.objects if obj.__class__.__name__ != "Vehicle")
            result.rehydration_by_site = {
                1: sum(1 for obj in result.objects if obj.__class__.__name__ == "Truck"),
                2: sum(1 for obj in result.objects if obj.__class__.__name__ == "ElectricCar"),
            }
            
            result.total_time = time.perf_counter() - t_start
            return result
        else:
            print(f"  Chiến lược: Tỏa nhánh SONG SONG (Parallel Fan-out) vì không có LIMIT\n")
            
            # ------------------------------------------------------------------
            # BƯỚC 1: TỎA NHÁNH SONG SONG (Parallel Fan-out Execution)
            # ------------------------------------------------------------------
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(sites_to_query)) as executor:
                futures = {
                    executor.submit(self._fetch_from_site, sid, class_name, field, value, op, year_min, year_max, None): sid
                    for sid in sites_to_query
                }
                for future in concurrent.futures.as_completed(futures):
                    sid, objects, elapsed, error = future.result()
                    result.timing[sid] = elapsed
                    if error:
                        result.errors[sid] = error
                        print(f"  [WARN] Site {sid} ({self.sites[sid]['name']}): {error} [{elapsed:.3f}s]")
                    else:
                        site_data[sid] = objects
                        print(f"  [OK]   Site {sid} ({self.sites[sid]['name']}): {len(objects)} đối tượng [{elapsed:.3f}s]")

        # ------------------------------------------------------------------
        # BƯỚC 2: HỢP NHẤT VÀ PHỤC HỒI (Merge & Rehydrate)
        # Giờ ta có 1 rổ mảnh Khung gầm (từ Site 0), và 1 rổ Động cơ, Pin (Từ Site 1, 2)
        # Phải dùng OID để ghép chúng lại với nhau (Giống phép JOIN trong SQL).
        # ------------------------------------------------------------------
        
        # 2.1 Lấy toàn bộ dữ liệu của Lớp con (Site 1, 2) và đánh index theo OID
        subclass_by_oid: Dict[str, Dict] = {}
        for sid in [1, 2]:
            for obj_data in site_data.get(sid, []):
                oid_str = obj_data.get("oid", "")
                if oid_str:
                    subclass_by_oid[oid_str] = (sid, obj_data)

        # 2.2 Quét qua Lớp cha (Site 0), lấy OID đối chiếu sang lớp con
        for base_data in site_data.get(0, []):
            oid_str = base_data.get("oid", "")
            if oid_str in subclass_by_oid:
                # NẾU KHỚP OID: Gộp 2 cái từ điển Dictionary lại làm 1 bằng cú pháp {**a, **b}
                print(f"  [REHYDRATION LOG] Đang join dữ liệu qua mạng cho OID={oid_str}...")
                source_site, subclass_data = subclass_by_oid[oid_str]
                merged = {**base_data, **subclass_data}
            else:
                # Nếu không khớp thì đây chỉ là chiếc Vehicle bình thường
                merged = base_data

            try:
                # Đưa cái từ điển vừa gộp qua Nhà máy (Factory) để đúc thành Đối tượng Python xịn
                obj = deserialize_object(merged)
                result.objects.append(obj)
            except Exception as exc:
                print(f"  [WARN] Không thể tái cấu trúc đối tượng {oid_str}: {exc}")

        # Đồng thời bao gồm các đối tượng lớp con không có dữ liệu cơ sở tại Site 0
        base_oids = {d.get("oid") for d in site_data.get(0, [])}
        for sid in [1, 2]:
            for obj_data in site_data.get(sid, []):
                oid_str = obj_data.get("oid", "")
                if oid_str not in base_oids:
                    try:
                        obj = deserialize_object(obj_data)
                        result.objects.append(obj)
                    except Exception:
                        pass
                        
        # ------------------------------------------------------------------
        # BƯỚC 4: LỌC CUỐI THEO CLASS VÀ LIMIT (Final filtering)
        # ------------------------------------------------------------------
        if class_name and class_name != "Vehicle":
            result.objects = [obj for obj in result.objects if obj.__class__.__name__ == class_name]

        if limit and limit > 0:
            result.objects = result.objects[:limit]

        # Tính toán lại thông số phục hồi chuẩn xác cho kết quả cuối
        result.rehydration_count = sum(1 for obj in result.objects if obj.__class__.__name__ != "Vehicle")
        result.rehydration_by_site = {
            1: sum(1 for obj in result.objects if obj.__class__.__name__ == "Truck"),
            2: sum(1 for obj in result.objects if obj.__class__.__name__ == "ElectricCar"),
        }

        result.total_time = time.perf_counter() - t_start
        return result

    # -------------------------------------------------------------------------
    # CÁC HÀM TIỆN ÍCH KHÁC
    # -------------------------------------------------------------------------
    def insert_vehicle(self, data: Dict[str, Any]) -> Optional[str]:
        """
        Thêm mới Xe. Nguyên lý: Khung xe đẩy vào Site 0. Thông số riêng đẩy vào Site 1/2.
        Cả 2 mảnh này được dán CHUNG MỘT OID DUY NHẤT.
        """
        cls_name = data.get("__class__", "Vehicle")
        site_map = {"Vehicle": 0, "Truck": 1, "ElectricCar": 2}
        site_id = site_map.get(cls_name, 0)

        try:
            site0_timeout = self.sites[0].get("timeout", 2.0)
            site_sub_timeout = self.sites[site_id].get("timeout", 2.0)
            
            # Luôn lưu Khung xe vào Site 0
            base_data = {
                "__class__": "Vehicle",
                "make": data.get("make"),
                "model": data.get("model"),
                "year": data.get("year"),
                "vin": data.get("vin"),
            }
            resp0 = self._session.post(self._site_url(0, "/insert"), json=base_data, timeout=site0_timeout)
            oid = resp0.json().get("oid")
            if not oid: return None

            # Nếu là xe đặc biệt, lấy OID đó ném tiếp sang Site 1/2
            if cls_name in ("Truck", "ElectricCar"):
                sub_data = {**data, "oid": oid, "__class__": cls_name}
                self._session.post(self._site_url(site_id, "/insert"), json=sub_data, timeout=site_sub_timeout)

            return oid
        except Exception as exc:
            print(f"  [FAIL] Lỗi chèn: {exc}")
            return None

    def sync_pending_schemas(self):
        """Hàm đồng bộ Lược đồ cho các Site vừa online trở lại (Catch-up / Eventual Consistency)."""
        import time
        for site_id, updates in self.pending_schemas.items():
            if not updates:
                continue
                
            # Thử ping xem site đã sống lại chưa
            is_alive, _ = self._ping_site(site_id)
            if not is_alive:
                continue
                
            print(f"\n[Eventual Consistency] Site {site_id} ĐÃ ONLINE! Đang đồng bộ {len(updates)} lược đồ còn thiếu...")
            successful_updates = []
            
            for update in updates:
                try:
                    site_timeout = self.sites[site_id].get("timeout", 2.0)
                    resp = self._session.post(
                        self._site_url(site_id, "/schema_evolve"),
                        json=update,
                        timeout=site_timeout,
                    )
                    resp.raise_for_status()
                    print(f"  -> Đã đồng bộ '{update['attribute']}' cho Site {site_id} thành công.")
                    successful_updates.append(update)
                except Exception as exc:
                    print(f"  -> LỖI đồng bộ '{update['attribute']}' cho Site {site_id}: {exc}")
                    break  # Dừng lại nếu lỗi, để lần sau thử tiếp
            
            # Xóa các update đã thành công khỏi hàng đợi
            for u in successful_updates:
                self.pending_schemas[site_id].remove(u)
                
            # Lưu lại xuống đĩa nếu có thay đổi
            if successful_updates:
                self._save_schema_registry()

    def schema_evolve(self, attribute: str, default_value: Any, new_version: str) -> Dict[str, Any]:
        """
        Tiến hóa Lược đồ theo mô hình Eventual Consistency (Nhất quán cuối cùng) 
        và Timestamp-based Conflict Resolution (Kiểm soát đồng thời).
        """
        import time
        
        # 1. CONFLICT RESOLUTION (Chống đụng độ)
        # Giả lập: Nếu người dùng nhập version <= version hiện tại -> Từ chối!
        # Đây là cách Coordinator ngăn chặn 2 Admin cập nhật đè lên nhau.
        try:
            current_v = tuple(map(int, self.global_schema_version.replace("v", "").split(".")))
            new_v = tuple(map(int, new_version.replace("v", "").split(".")))
            if new_v <= current_v:
                return {
                    "error": f"Conflict Detected! Phiên bản yêu cầu ({new_version}) phải lớn hơn phiên bản hiện tại ({self.global_schema_version})"
                }
        except Exception:
            pass # Bỏ qua nếu version không thể parse (vd: v1.x)
            
        # Cập nhật Global Schema Registry
        self.global_schema_version = new_version
        update_payload = {
            "attribute": attribute,
            "default": default_value,
            "new_version": new_version,
            "timestamp": time.time()
        }
        self.schema_changelog.append(update_payload)
        
        print(f"\n[Global Schema Registry] Đã nâng cấp Lược đồ lên v{new_version}.")
        print(f"  Thuộc tính mới: '{attribute}' = {default_value!r}")
        
        # Thêm vào hàng đợi Pending của TẤT CẢ các site
        for site_id in self.sites:
            self.pending_schemas[site_id].append(update_payload)
            
        # Lưu Schema Registry xuống đĩa
        self._save_schema_registry()

        # 2. EVENTUAL CONSISTENCY PROPAGATION
        # Gửi lập tức đến các site đang sống
        results = {"decision": "EVENTUAL_CONSISTENCY", "sites": {}}
        
        for site_id in self.sites:
            try:
                site_timeout = self.sites[site_id].get("timeout", 2.0)
                resp = self._session.post(
                    self._site_url(site_id, "/schema_evolve"),
                    json=update_payload,
                    timeout=site_timeout,
                )
                results["sites"][site_id] = resp.json()
                # Xóa khỏi hàng đợi vì đã update thành công
                self.pending_schemas[site_id].remove(update_payload)
                self._save_schema_registry()
                print(f"  [OK] Site {site_id} cập nhật thành công.")
            except Exception as exc:
                results["sites"][site_id] = {"error": str(exc), "status": "PENDING"}
                print(f"  [OFFLINE] Site {site_id} không phản hồi. Đã đưa vào Hàng đợi Catch-up.")
        
        return results

    def get_site_stats(self) -> Dict[int, Any]:
        """Hàm lấy thống kê từ các site."""
        # Đồng bộ Schema Eventual Consistency
        self.sync_pending_schemas()
        
        stats = {}
        for site_id in self.sites:
            try:
                resp = self._session.get(self._site_url(site_id, "/stats"), timeout=2.0)
                stats[site_id] = resp.json()
            except Exception as exc:
                stats[site_id] = {"error": str(exc)}
        return stats
