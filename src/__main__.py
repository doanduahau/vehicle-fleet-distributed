"""
Điểm nổ (Entry point) của gói mã nguồn src
==========================================
Vai trò của file:
    Giống như file `__init__.py`, file `__main__.py` là một thủ thuật đặc biệt của Python.
    Nó cho phép bạn chạy CẢ MỘT THƯ MỤC như thể thư mục đó là một kịch bản (script).

Cách dùng:
    Thay vì phải gõ lệnh dài dòng:
        python src/site_server.py 0
    Nhờ có file `__main__.py` này, bạn có thể gõ lệnh ngắn gọn hơn bằng cờ `-m` (module):
        python -m src 0
    
    Khi bạn gõ lệnh trên, Python sẽ tự động chui vào thư mục `src`, tìm file `__main__.py` 
    để chạy đầu tiên. File này sẽ tự động mồi (kích hoạt) máy chủ SiteServer.
"""

# =============================================================================
# CÁC THƯ VIỆN ĐƯỢC NHÚNG VÀO (IMPORTS)
# =============================================================================

# Import lớp SiteServer: Trái tim của Worker Node, dùng để khởi tạo và chạy máy chủ Flask
from src.site_server import SiteServer

# Import argparse: Thư viện chuẩn của Python giúp phân tích các tham số mà người dùng
# gõ trên màn hình đen Terminal. Ví dụ khi người dùng gõ chữ "0" ở cuối lệnh.
import argparse

# Import sys: Thư viện giao tiếp với hệ thống (mặc dù file này chưa dùng trực tiếp đến 
# các hàm chuyên sâu của nó, nhưng là thư viện thiết yếu đi kèm khi làm việc với dòng lệnh)
import sys

# =============================================================================
# QUY TRÌNH THỰC THI CHÍNH KHI BẬT MODULE
# =============================================================================

# Khởi tạo một cái máy đọc tham số (Bộ phân tích cú pháp)
parser = argparse.ArgumentParser()

# Cấu hình cho máy đọc biết: "Tôi đang chờ 1 tham số có tên là 'site_id'.
# Tham số này phải là số nguyên (type=int) và CHỈ ĐƯỢC PHÉP nằm trong 3 số: 0, 1 hoặc 2".
parser.add_argument("site_id", type=int, choices=[0, 1, 2])

# Ra lệnh cho máy đọc phân tích dòng lệnh mà người dùng vừa gõ và lưu kết quả vào biến 'args'
# Nếu người dùng gõ sai (ví dụ gõ số 3), nó sẽ tự động chửi lỗi và tắt chương trình ngay tại đây.
args = parser.parse_args()

# Khởi tạo Đối tượng Máy chủ Site (SiteServer) với ID vừa đọc được.
# Nếu args.site_id = 0, nó sẽ dựng lên Master Node.
# Nếu args.site_id = 1 hoặc 2, nó sẽ dựng lên Worker Node.
server = SiteServer(args.site_id)

# Kích hoạt máy chủ, bắt đầu mở cổng mạng (Port 5000/5001/5002) và lắng nghe Request từ bên ngoài.
server.run()
