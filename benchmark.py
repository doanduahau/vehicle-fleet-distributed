import sys
import time
import requests
import matplotlib.pyplot as plt
import numpy as np

# Khắc phục lỗi hiển thị tiếng Việt trên Terminal Windows
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except AttributeError:
    pass

def run_benchmark():
    print("=" * 65)
    print(" BẮT ĐẦU BENCHMARK HỆ THỐNG PHÂN TÁN (PERFORMANCE SCALING)")
    print("=" * 65)
    
    # Số lượng bản ghi để benchmark (thay đổi linh hoạt tùy CSDL)
    # Vì dữ liệu mẫu là 1500, ta sẽ dùng các mốc: 10, 50, 100, 500, 1000, 1500
    object_counts = [10, 50, 100, 500, 1000, 1500]
    
    local_fetch_times = []
    network_latency_times = []
    rehydration_times = []
    total_times = []
    
    for count in object_counts:
        print(f"\n[Mốc {count} đối tượng] Đang gửi truy vấn...")
        
        # Gọi API với limit
        # Lưu ý: Hàm global search hiện tại không nhận limit trực tiếp vào DB,
        # nhưng chúng ta có thể truyền limit vào API để giới hạn kết quả phân tích.
        # Để chính xác, ta gọi trực tiếp API /global/search
        try:
            start_time = time.time()
            # Giả lập truy vấn Đa hình
            resp = requests.get(f"http://127.0.0.1:5000/global/search?limit={count}", timeout=30.0)
            end_time = time.time()
            
            data = resp.json()
            timing = data.get("timing", {})
            rehydration_count = data.get("rehydration_count", 0)
            
            # Thời gian lấy cục bộ (Site 0)
            t_local = timing.get("0", 0.0)
            
            # Thời gian mạng (Max của Site 1 và Site 2)
            t_remote_1 = timing.get("1", 0.0)
            t_remote_2 = timing.get("2", 0.0)
            t_network = max(t_remote_1, t_remote_2)
            
            t_total = data.get("total_time", end_time - start_time)
            
            # Thời gian rehydration xấp xỉ bằng t_total - max(t_local, t_network)
            t_rehydration = max(0, t_total - max(t_local, t_network))
            
            local_fetch_times.append(t_local * 1000)      # Đổi sang ms
            network_latency_times.append(t_network * 1000)
            rehydration_times.append(t_rehydration * 1000)
            total_times.append(t_total * 1000)
            
            print(f"  Thành công: {len(data.get('results', []))} đối tượng.")
            print(f"  + Local Fetch:   {t_local * 1000:.1f} ms")
            print(f"  + Network (Max): {t_network * 1000:.1f} ms")
            print(f"  + Rehydration:   {t_rehydration * 1000:.1f} ms")
            print(f"  = Total Time:    {t_total * 1000:.1f} ms")
            
        except Exception as e:
            print(f"  [LỖI] Benchmark thất bại tại mốc {count}: {e}")
            break

    # Vẽ biểu đồ nếu thu thập đủ dữ liệu
    if len(total_times) > 0:
        print("\nĐang xuất biểu đồ ra file 'benchmark_results.png'...")
        
        counts = object_counts[:len(total_times)]
        x = np.arange(len(counts))
        width = 0.5
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        p1 = ax.bar(x, local_fetch_times, width, label='Local Fetch (Site 0)', color='#2ecc71')
        p2 = ax.bar(x, network_latency_times, width, bottom=local_fetch_times, label='Network Latency (Remote)', color='#3498db')
        
        # Rehydration nằm trên cùng
        bottom_rehydration = np.array(local_fetch_times) + np.array(network_latency_times)
        p3 = ax.bar(x, rehydration_times, width, bottom=bottom_rehydration, label='Object Rehydration (Join)', color='#e74c3c')
        
        # Thêm đường tổng thời gian
        ax.plot(x, total_times, color='black', marker='o', linestyle='-', linewidth=2, label='Total Response Time')
        
        # Trang trí đồ thị
        ax.set_ylabel('Thời gian phản hồi (ms)')
        ax.set_xlabel('Số lượng đối tượng (Objects)')
        ax.set_title('Đánh giá Hiệu năng Hệ quản trị CSDL Phân tán\n(Phân mảnh dọc & Khôi phục đối tượng đa hình)')
        ax.set_xticks(x)
        ax.set_xticklabels([f"{c} objs" for c in counts])
        ax.legend()
        
        # Thêm số liệu lên các điểm total_times
        for i, total in enumerate(total_times):
            ax.annotate(f"{total:.0f}ms", (x[i], total_times[i] + max(total_times)*0.02), ha='center')

        plt.tight_layout()
        plt.savefig("benchmark_results.png", dpi=300)
        print("Đã lưu 'benchmark_results.png'. Bạn có thể copy hình này vào báo cáo!")

if __name__ == "__main__":
    run_benchmark()
