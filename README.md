# TLCN
Các bước triển khai hệ thống Lakehouse Platform
Bước 1:
khởi động máy ảo và chạy file data.ovf đã được gửi qua link drive
Bước 2:
khởi chạy mysql,kafka,spark,minIO trong máy ảo
Bước 3:
nạp file mysql-source.json lên kafka connect,chạy file spark_delta_lake_manual.py để tiến hành chạy realtime đổ dữ liệu về MinIO
Bước 4:
chạy UI streamlit file HomePage.py để tiến hành đưa dữ liệu từ tệp csv về MinIO.
Bước 5:
vào kho MinIO để kiểm tra xem dữ liệu đã được đổ về hay chưa để tiếp tục tiền xử lý dữ liệu
Bước 6:
mở file amazon_product.ipynb là file tiền xử lý dữ liệu  
Bước 7:
các chart thống kê sẽ được hiển thị trên UI streamlit thông qua file Analysis_chart.py