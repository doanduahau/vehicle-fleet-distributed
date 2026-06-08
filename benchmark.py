import sys
import time
import requests
import csv
import matplotlib
matplotlib.use('Agg')
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
    object_counts = [10, 25, 50, 100, 250, 500]
    num_runs = 5
    
    parallel_fetch_times = []
    rehydration_times = []
    total_times = []
    
    csv_data = []
    csv_headers = ["Object Count", "Parallel Fetch (ms)", "Rehydration (ms)", "Total Time (ms)"]
    
    for count in object_counts:
        print(f"\n[Mốc {count} đối tượng] Đang gửi truy vấn {num_runs} lần để lấy trung bình...")
        
        avg_t_parallel = 0.0
        avg_t_rehydration = 0.0
        avg_t_total = 0.0
        success_count = 0
        returned_objs = 0
        
        for run in range(num_runs):
            try:
                start_time = time.time()
                resp = requests.get(f"http://127.0.0.1:5000/global/search?limit={count}", timeout=30.0)
                end_time = time.time()
                
                data = resp.json()
                timing = data.get("timing", {})
                
                t_local = timing.get("0", 0.0)
                t_remote_1 = timing.get("1", 0.0)
                t_remote_2 = timing.get("2", 0.0)
                
                # T_parallel = max(T_site0, T_site1, T_site2)
                t_parallel = max(t_local, t_remote_1, t_remote_2)
                t_total = data.get("total_time", end_time - start_time)
                
                # T_rehydration = T_total - T_parallel
                t_rehydration = max(0, t_total - t_parallel)
                
                avg_t_parallel += t_parallel
                avg_t_rehydration += t_rehydration
                avg_t_total += t_total
                success_count += 1
                
                # Lưu số lượng obj thực tế trả về từ run cuối
                if run == num_runs - 1:
                    returned_objs = len(data.get('objects', []))
                
            except Exception as e:
                print(f"  [LỖI] Run {run+1} thất bại: {e}")
        
        if success_count > 0:
            avg_t_parallel = (avg_t_parallel / success_count) * 1000
            avg_t_rehydration = (avg_t_rehydration / success_count) * 1000
            avg_t_total = (avg_t_total / success_count) * 1000
            
            parallel_fetch_times.append(avg_t_parallel)
            rehydration_times.append(avg_t_rehydration)
            total_times.append(avg_t_total)
            
            csv_data.append([count, avg_t_parallel, avg_t_rehydration, avg_t_total])
            
            print(f"  Thành công: ~{returned_objs} đối tượng.")
            print(f"  + Parallel Fetch (Max of all sites): {avg_t_parallel:.1f} ms")
            print(f"  + Rehydration Cost:                  {avg_t_rehydration:.1f} ms")
            print(f"  = Total Time:                        {avg_t_total:.1f} ms")
        else:
            print(f"  [LỖI] Benchmark thất bại hoàn toàn tại mốc {count}.")

    # Xuất CSV
    if csv_data:
        print("\nĐang xuất kết quả ra file 'benchmark_results.csv'...")
        with open('benchmark_results.csv', mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(csv_headers)
            writer.writerows(csv_data)

    # Vẽ biểu đồ
    if len(total_times) > 0:
        print("Đang xuất biểu đồ ra file 'benchmark_results.png'...")
        
        counts = object_counts[:len(total_times)]
        x = np.arange(len(counts))
        width = 0.5
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Biểu đồ chỉ còn 2 stack: Parallel Fetch và Rehydration
        p1 = ax.bar(x, parallel_fetch_times, width, label='Parallel Fetch (Max of Sites)')
        p2 = ax.bar(x, rehydration_times, width, bottom=parallel_fetch_times, label='Object Rehydration (Join)')
        
        # Thêm đường tổng thời gian
        ax.plot(x, total_times, marker='o', linestyle='-', linewidth=2, label='Total Response Time')
        
        # Trang trí đồ thị
        ax.set_ylabel('Thời gian phản hồi trung bình (ms)')
        ax.set_xlabel('Số lượng đối tượng (Objects)')
        ax.set_title('Đánh giá Hiệu năng CSDL Phân tán (T_total ≈ T_parallel + T_rehydration)')
        ax.set_xticks(x)
        ax.set_xticklabels([f"{c} objs" for c in counts])
        ax.legend()
        
        # Thêm số liệu lên các điểm total_times
        for i, total in enumerate(total_times):
            ax.annotate(f"{total:.0f}ms", (x[i], total_times[i] + max(total_times)*0.02), ha='center')

        plt.tight_layout()
        plt.savefig("benchmark_results.png", dpi=300)
        print("Hoàn tất! Bạn có thể copy 'benchmark_results.png' và 'benchmark_results.csv' vào báo cáo!")

if __name__ == "__main__":
    run_benchmark()
