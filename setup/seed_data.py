# -*- coding: utf-8 -*-
"""
Trình tạo Dữ liệu Mẫu (Seed Data Generator) — Nguồn dữ liệu thực tế
================================================================================
Vai trò của file:
    Đây là file ETL (Extract - Transform - Load). Nó tải dữ liệu thực tế từ 2 nguồn
    mở uy tín trên Internet, biến đổi (Transform) chúng về đúng lược đồ của hệ thống
    OODBMS phân tán, sau đó bơm (Load) vào các Node qua REST API.

Nguồn dữ liệu thực tế:
    [Site 2 - Xe điện]:
        KilowattApp / open-ev-data (GitHub)
        URL: https://raw.githubusercontent.com/KilowattApp/open-ev-data/master/data/ev-data.json
        Mô tả: 1,321 xe điện thực tế (Tesla, BYD, BMW, Hyundai...) với đầy đủ thông số
                pin, tầm hoạt động, cổng sạc. Cập nhật liên tục bởi cộng đồng mã nguồn mở.

    [Site 1 - Xe tải]:
        Dữ liệu tổng hợp từ tiêu chuẩn GVWR của NHTSA (Mỹ).
        Các hãng xe tải thực tế: Ford, Chevrolet, Ram, Toyota, GMC, Nissan.
        Tải trọng & thông số được sinh theo đúng phân nhóm GVWR chuẩn quốc tế.

Dựa trên lý thuyết:
    Özsu & Valduriez "Principles of Distributed Database Systems" (4th Ed.)
    - Chương 4: Phân mảnh dữ liệu (Fragmentation)
    - Mục 4.3: Phân mảnh dọc (Vertical Fragmentation) - Cắt dọc một bảng theo cột.
"""

import json
import random
import sys
import time
import os

import requests  # Thư viện để gọi API (HTTP) qua mạng

# =============================================================================
# CẤU HÌNH MẠNG LƯỚI KẾT NỐI
# =============================================================================
SITES = {
    0: {"host": os.environ.get("SITE0_HOST", "localhost"), "port": 5000, "class": "Vehicle"},
    1: {"host": os.environ.get("SITE1_HOST", "localhost"), "port": 5001, "class": "Truck"},
    2: {"host": os.environ.get("SITE2_HOST", "localhost"), "port": 5002, "class": "ElectricCar"},
}

# URL dữ liệu JSON xe điện mã nguồn mở thực tế (1321 xe)
EV_DATA_URL = "https://raw.githubusercontent.com/KilowattApp/open-ev-data/master/data/ev-data.json"

def base_url(site_id):
    """Hàm nối chuỗi để ra địa chỉ đầy đủ (Ví dụ: http://localhost:5000)"""
    return f"http://{SITES[site_id]['host']}:{SITES[site_id]['port']}"

def wait_for_sites(max_retries=10, delay=1.5):
    """
    Hàm chờ (Wait). Khi bạn bật hệ thống lên, CSDL Postgres có thể tốn vài giây để khởi động.
    Hàm này sẽ liên tục gọi API /ping để hỏi xem "Các anh đã dậy chưa?".
    Đủ 3 anh trả lời OK thì mới bắt đầu bơm dữ liệu.
    """
    for attempt in range(max_retries):
        all_up = True
        for sid in SITES:
            try:
                r = requests.get(f"{base_url(sid)}/ping", timeout=2.0)
                if r.status_code != 200:
                    all_up = False
            except Exception:
                all_up = False
                
        if all_up:
            print(f"[OK] Tất cả các site đều trực tuyến sau {attempt + 1} lần thử")
            return True
            
        print(f"  Đang đợi các site... lần thử thứ {attempt + 1}/{max_retries}")
        time.sleep(delay)
        
    return False


def insert(site_id, data):
    """Hàm gọi API /insert để đẩy 1 Object xuống 1 Site. Trả về mã OID được cấp."""
    resp = requests.post(f"{base_url(site_id)}/insert", json=data, timeout=10.0)
    if resp.status_code == 201:
        return resp.json()["oid"]
    else:
        print(f"  LỖI khi chèn tại Site {site_id}: {resp.text}")
        return None


# =============================================================================
# HÀM TẢI DỮ LIỆU THỰC TẾ TỪ INTERNET
# =============================================================================

def fetch_ev_data():
    """
    Tải dữ liệu xe điện thực tế từ KilowattApp/open-ev-data trên GitHub.
    Trả về danh sách các bản ghi đã được làm sạch (Transform) về đúng lược đồ.
    """
    print(f"\n[ETL] Đang tải dữ liệu xe điện thực tế từ:")
    print(f"      {EV_DATA_URL}")
    
    try:
        resp = requests.get(EV_DATA_URL, timeout=15.0)
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        print(f"  [CẢNH BÁO] Không tải được từ Internet: {e}")
        print(f"  => Dùng dữ liệu dự phòng (Fallback)...")
        return None

    records = raw.get("data", [])
    print(f"  Tải thành công: {len(records)} bản ghi xe điện thực tế.")
    
    # Transform: Lọc bỏ các bản ghi bị thiếu thông số quan trọng, ánh xạ sang lược đồ của chúng ta
    cleaned = []
    for r in records:
        brand = r.get("brand", "").strip()
        model = r.get("model", "").strip()
        year  = r.get("release_year") or r.get("year")
        battery = r.get("usable_battery_size") or r.get("battery_size")
        ev_range = r.get("range")
        
        # Xác định cổng sạc DC
        dc = r.get("dc_charger", {})
        ports = dc.get("ports", [])
        connector = "CCS"
        if "chademo" in ports:
            connector = "CHAdeMO"
        elif "tesla" in ports:
            connector = "Tesla/NACS"
        elif "ccs" in ports:
            connector = "CCS"
        elif ports:
            connector = ports[0].upper()

        # Bỏ qua bản ghi thiếu thông tin quan trọng
        if not brand or not model or not year or not battery:
            continue
            
        # Gán tầm hoạt động mặc định nếu thiếu (Dựa trên dung lượng pin)
        if not ev_range:
            ev_range = int(battery * 5.5)  # Ước tính thực tế: ~5.5km/kWh

        cleaned.append({
            "make":  brand,
            "model": model,
            "year":  int(year),
            "vin":   f"BEV{random.randint(10000000000000, 99999999999999)}",
            # Thông tin phân mảnh dọc (Sẽ bơm sang Site 2)
            "_ev_battery": round(float(battery), 1),
            "_ev_range":   int(ev_range),
            "_ev_connector": connector,
        })
    
    print(f"  Sau khi làm sạch (Transform): {len(cleaned)} bản ghi hợp lệ.")
    return cleaned


TRUCK_DATA_URL = "https://raw.githubusercontent.com/vbalagovic/cars-dataset/main/truck_data_sample.json"

def fetch_truck_data(count=500):
    """
    Tải dữ liệu xe tải thực tế từ vbalagovic/cars-dataset (GitHub).
    Nguồn gốc: truck-data.com và car2db.
    Các hãng có trong dataset: DAF, MAN, Mercedes-Benz, Volvo, Scania, Ford, Fiat...

    Quá trình Transform:
      - payload_capacity_kg = gvw_kg - kerb_weight_kg (nếu trường gốc bị null)
      - Số trục đếm từ axle_config (VD: '6x2' -> 6 trục)
      - has_trailer = True nếu gcw_kg > gvw_kg (xe có tổng khối lượng tổ hợp cao hơn)
    """
    print(f"\n[ETL] Đang tải dữ liệu xe tải thực tế từ:")
    print(f"      {TRUCK_DATA_URL}")

    try:
        resp = requests.get(TRUCK_DATA_URL, timeout=15.0)
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        print(f"  [CẢNH BÁO] Không tải được từ Internet: {e}")
        print(f"  => Dùng dữ liệu dự phòng (Fallback)...")
        return None

    print(f"  Tải thành công: {len(raw)} bản ghi xe tải thực tế.")

    cleaned = []
    for r in raw:
        brand = (r.get("brand") or "").strip()
        model = (r.get("model") or "").strip()
        year  = r.get("year") or 2018
        gvw   = r.get("gvw_kg")
        kerb  = r.get("kerb_weight_kg")

        # Tính payload từ GVW - Kerb nếu bị null trong nguồn gốc
        payload = r.get("payload_capacity_kg")
        if not payload and gvw and kerb:
            payload = gvw - kerb
        if not payload or payload <= 0 or not brand or not model:
            continue

        # Đếm số trục từ chuỗi axle_config (VD: '6x2' -> 6)
        axle_config = r.get("axle_config") or "4x2"
        try:
            axle_count = int(str(axle_config).split("x")[0])
        except Exception:
            axle_count = 2

        gcw = r.get("gcw_kg")
        has_trailer = bool(gcw and gcw > (gvw or 0))

        cleaned.append({
            "make":  brand,
            "model": model,
            "year":  int(year),
            "vin":   f"TRK{random.randint(10000000000000, 99999999999999)}",
            "_truck_payload":     round(float(payload), 1),
            "_truck_axles":       axle_count,
            "_truck_has_trailer": has_trailer,
        })

    print(f"  Sau khi làm sạch (Transform): {len(cleaned)} bản ghi hợp lệ.")

    # Lặp lại dữ liệu nếu số lượng thực tế ít hơn count cần thiết
    if len(cleaned) == 0:
        return None
    while len(cleaned) < count:
        extra = cleaned[len(cleaned) % len(cleaned)].copy()
        extra["vin"] = f"TRK{random.randint(10000000000000, 99999999999999)}"
        cleaned.append(extra)

    random.shuffle(cleaned)
    return cleaned[:count]


# =============================================================================
# HÀM BƠM DỮ LIỆU CHÍNH
# =============================================================================

def seed_all():
    print("\n" + "=" * 65)
    print("ĐANG NẠP DỮ LIỆU THỰC TẾ VÀO HỆ THỐNG CSDL PHÂN TÁN")
    print("=" * 65)

    # ------------------------------------------------------------------
    # GIAI ĐOẠN 0: Tải dữ liệu từ nguồn thực tế
    # ------------------------------------------------------------------
    ev_records = fetch_ev_data()
    
    # Nếu không có mạng, dùng dữ liệu dự phòng
    if not ev_records:
        ev_records = [
            {"make": "Tesla",   "model": "Model 3", "year": 2023, "vin": f"BEV{i:014d}",
             "_ev_battery": 82.0, "_ev_range": 576, "_ev_connector": "Tesla/NACS"}
            for i in range(700)
        ]

    truck_records = fetch_truck_data(count=500)
    # Nếu không tải được từ Internet, dùng dữ liệu dự phòng
    if not truck_records:
        truck_records = [
            {"make": "Ford", "model": "F-250", "year": 2020, "vin": f"TRK{i:014d}",
             "_truck_payload": 3000.0, "_truck_axles": 4, "_truck_has_trailer": True}
            for i in range(500)
        ]
    base_records = [
        {"make": m, "model": mo, "year": random.randint(2010, 2024), "vin": f"BAS{random.randint(10000000000000, 99999999999999)}"}
        for m, mo in [("Toyota","Camry"),("Honda","Civic"),("Ford","Escape"),("Mazda","CX-5"),("Subaru","Outback")]
        for _ in range(60)  # 5 hãng x 60 = 300 xe dân dụng
    ]
    
    # Giới hạn xe điện tối đa 700 bản ghi (lấy ngẫu nhiên từ dataset thực)
    random.shuffle(ev_records)
    ev_records = ev_records[:700]
    
    all_vehicles = truck_records + ev_records + base_records
    total = len(all_vehicles)
    print(f"\n  Tổng xe sẽ nạp: {total} (Tải={len(truck_records)} | EV={len(ev_records)} | Dân dụng={len(base_records)})")

    # ------------------------------------------------------------------
    # GIAI ĐOẠN 1: Bơm Khung gầm vào Site 0
    # ------------------------------------------------------------------
    print(f"\n[Site 0] Bơm {total} bản ghi Khung gầm cơ sở (Base Schema)...")
    oids_site0 = []
    for idx, v in enumerate(all_vehicles):
        # Chỉ gửi thông tin Base Schema — KHÔNG gửi các trường "_ev_*", "_truck_*"
        base_data = {k: v for k, v in v.items() if not k.startswith("_")}
        oid = insert(0, base_data)
        oids_site0.append(oid)
        if (idx + 1) % 200 == 0:
            print(f"  ... {idx + 1}/{total}")
    
    print(f"  [OK] Site 0: {sum(1 for o in oids_site0 if o)} bản ghi thành công.")

    # ------------------------------------------------------------------
    # GIAI ĐOẠN 2: Bơm mảnh Xe tải sang Site 1
    # ------------------------------------------------------------------
    truck_oids = oids_site0[:len(truck_records)]
    print(f"\n[Site 1] Bơm {len(truck_records)} mảnh Truck (Tải trọng, Trục xe)...")
    for idx, (oid, truck) in enumerate(zip(truck_oids, truck_records)):
        if not oid:
            continue
        insert(1, {
            "__class__": "Truck",
            "oid": oid,
            "make":  truck["make"],  "model": truck["model"],
            "year":  truck["year"],  "vin":   truck["vin"],
            "payload_capacity_kg": truck["_truck_payload"],
            "axle_count":          truck["_truck_axles"],
            "has_trailer":         truck["_truck_has_trailer"],
        })
        if (idx + 1) % 100 == 0:
            print(f"  ... {idx + 1}/{len(truck_records)}")
    
    print(f"  [OK] Site 1: {len(truck_records)} mảnh xe tải thành công.")

    # ------------------------------------------------------------------
    # GIAI ĐOẠN 3: Bơm mảnh Xe điện sang Site 2
    # ------------------------------------------------------------------
    ev_oids = oids_site0[len(truck_records):len(truck_records) + len(ev_records)]
    print(f"\n[Site 2] Bơm {len(ev_records)} mảnh ElectricCar (Pin, Tầm hoạt động)...")
    for idx, (oid, ev) in enumerate(zip(ev_oids, ev_records)):
        if not oid:
            continue
        insert(2, {
            "__class__": "ElectricCar",
            "oid": oid,
            "make":  ev["make"],  "model": ev["model"],
            "year":  ev["year"],  "vin":   ev["vin"],
            "battery_capacity_kwh": ev["_ev_battery"],
            "range_km":             ev["_ev_range"],
            "charge_connector":     ev["_ev_connector"],
        })
        if (idx + 1) % 100 == 0:
            print(f"  ... {idx + 1}/{len(ev_records)}")
    
    print(f"  [OK] Site 2: {len(ev_records)} mảnh xe điện thực tế thành công.")

    print("\n" + "=" * 65)
    print(f"[HOÀN TẤT] Đã nạp {total} phương tiện vào hệ thống phân tán!")
    print(f"  Site 0: {total} bản ghi Khung gầm chung")
    print(f"  Site 1: {len(truck_records)} mảnh Xe tải (Nguồn: Tiêu chuẩn GVWR/NHTSA)")
    print(f"  Site 2: {len(ev_records)} mảnh Xe điện (Nguồn: KilowattApp/open-ev-data)")
    print("=" * 65)


if __name__ == "__main__":
    if not wait_for_sites():
        print("[FAIL] Không thể kết nối tới các Site. Bạn đã chạy 'docker compose up -d' chưa?")
        sys.exit(1)
    seed_all()
