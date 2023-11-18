import streamlit as st
import streamlit as st1
import streamlit as st2
import pandas as pd
import pymysql
import re
st.set_page_config(page_title="Import_file_csv", page_icon="🌐")
page_bg_img = """
<style>
[data-testid="stAppViewContainer"] {
    background-image: url("");
    background-size: 100% 100%;
}
[data-testid="stHeader"]{
    background: rgba(0,0,0,0);
}
[data-testid="stToolbar"]{
    right:2rem;
}
[data-testid="stSidebar"] > div:first-child {
    background-image: url("https://cdn.sforum.vn/sforum/wp-content/uploads/2020/04/mountain-valley-iphone-wallpaper-axellvak-sunset-deer-scaled.jpg");
     background-size: 100% 100%;
}
</style>
"""
st.markdown(page_bg_img, unsafe_allow_html=True)
# Khởi tạo ứng dụng Streamlit
st.title('CSV to MySQL')

# Cho phép người dùng tải lên một tệp CSV
uploaded_file = st.file_uploader("Chọn một tệp CSV", type=["csv"])

if uploaded_file is not None:
    # Đọc dữ liệu từ tệp CSV bằng Pandas
    data = pd.read_csv(uploaded_file)
    # Xử lý giá trị NaN: thay thế NaN bằng giá trị mặc định (ví dụ: 0)
    data = data.fillna(0)  
    # Hiển thị dữ liệu đọc từ tệp CSV
    st.write("Dữ liệu từ tệp CSV:")
    st.write(data)
    
    # Thay đổi các thông tin kết nối
    mysql_host = '192.168.1.3'
    mysql_user = 'hoang2'
    mysql_password = 'Hoang@123'
    mysql_db = 'amazon'
    
    # Tạo tên bảng từ tên tệp CSV và loại bỏ dấu cách và ký tự đặc biệt
    table_name = uploaded_file.name
    table_name = re.sub(r'[^a-zA-Z0-9]', '_', table_name)
    
    # Kết nối tới cơ sở dữ liệu MySQL
    conn = pymysql.connect(host=mysql_host, user=mysql_user, password=mysql_password, database=mysql_db)
    cursor = conn.cursor()

    # Tạo bảng dựa trên kiểu dữ liệu trong tệp CSV
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    create_table_sql += "id INT PRIMARY KEY AUTO_INCREMENT, " 
    for column, dtype in zip(data.columns, data.dtypes):
        if dtype == 'int64':
            sql_type = 'INTEGER'
        elif dtype == 'float64':
            sql_type = 'FLOAT'
        else:
            sql_type = 'TEXT'
        create_table_sql += f"{column} {sql_type}, "
    create_table_sql = create_table_sql.rstrip(', ')
    create_table_sql += ");"

    cursor.execute(create_table_sql)
    

    # Thực hiện câu lệnh INSERT INTO để nhập dữ liệu từ tệp CSV vào cơ sở dữ liệu
    for index, row in data.iterrows():
        columns = ', '.join(row.index)
        values = ', '.join(['%s'] * len(row))
        insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        cursor.execute(insert_sql, tuple(row))

    # Lưu thay đổi và đóng kết nối
    conn.commit()
    conn.close()
    
    st.success(f"Dữ liệu đã được chèn vào bảng {table_name} trong cơ sở dữ liệu MySQL.")
    # Thêm biến cờ để theo dõi trạng thái quá trình chạy
    # process_started = False

    # # Khi nút "PROCESS" được nhấp
    # if st.button("PROCESS"):
    #     process_started = True

    # # Thực hiện công việc Spark và Delta Lake khi biến cờ là True
    # if process_started:
    #     # Gọi hàm thực hiện công việc Spark và Delta Lake
    #     spark_write.main()  # Thay thế bằng tên hàm thích hợp từ tệp spark_write
    #     spark_read.main()
# Add a delete button
delete_button = st.button("Delete Data")

if delete_button:
    mysql_host = '192.168.1.3'
    mysql_user = 'hoang2'
    mysql_password = 'Hoang@123'
    mysql_db = 'amazon'
    # Connect to the MySQL database
    conn = pymysql.connect(host=mysql_host, user=mysql_user, password=mysql_password, database=mysql_db)
    cursor = conn.cursor()

    # Delete all data from the table
    delete_sql = f"DELETE FROM {table_name};"
    cursor.execute(delete_sql)

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

    st.success(f"All data has been deleted from the {table_name} table.")
# Option selectbox for actions
action = st.selectbox("Choose an action:", ["Add", "Delete", "Edit"])
if action == "Add":
    # Add input fields for adding data
    st.write("Add Data:")
    add_data = {}

    for column in data.columns:
        add_data[column] = st.text_input(column)

    # Add button for adding data
    add_button = st.button("Add Data")

    if add_button:
        # Connect to the MySQL database
        conn = pymysql.connect(host=mysql_host, user=mysql_user, password=mysql_password, database=mysql_db)
        cursor = conn.cursor()

        # Insert the new data into the table
        columns = ', '.join(add_data.keys())
        values = ', '.join(['%s'] * len(add_data.values()))
        insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        cursor.execute(insert_sql, tuple(add_data.values()))

        # Commit the changes and close the connection
        conn.commit()
        conn.close()

        st.success(f"Data has been added to the {table_name} table.")

# elif action == "Delete":
#     # Add input fields for deleting data
#     st.write("Delete Data:")
#     delete_data = {}

#     for column in data.columns:
#         delete_data[column] = st.text_input(column)

#     # Delete button for deleting data
#     delete_button = st.button("Delete Data")
#     if delete_button:
#     # Connect to the MySQL database
#         conn = pymysql.connect(host=mysql_host, user=mysql_user, password=mysql_password, database=mysql_db)
#         cursor = conn.cursor()

#         # Construct the DELETE statement based on the input data
#         delete_sql = f"DELETE FROM {table_name} WHERE "

#         conditions = []
#         values = []
#         for column, value in delete_data.items():
#             conditions.append(f"{column} = %s")
#             values.append(value)

#         delete_sql += " AND ".join(conditions)

#         # Execute the DELETE statement
#         cursor.execute(delete_sql, tuple(values))

#         # Commit the changes and close the connection
#         conn.commit()
#         conn.close()

#         st.success(f"Data has been deleted from the {table_name} table.")

elif action == "Edit":
    # Add input fields for editing data
    st.write("Edit Data:")
    edit_data = {}

    for column in data.columns:
        edit_data[column] = st.text_input(column)

    # Edit button for editing data
    edit_button = st.button("Edit Data")

    if edit_button:
        # Connect to the MySQL database
        conn = pymysql.connect(host=mysql_host, user=mysql_user, password=mysql_password, database=mysql_db)
        cursor = conn.cursor()

        # Construct the UPDATE statement based on the input data
        update_sql = f"UPDATE {table_name} SET "

        updates = []
        for column, value in edit_data.items():
            updates.append(f"{column} = '{value}'")

        update_sql += ", ".join(updates)

        # Execute the UPDATE statement
        cursor.execute(update_sql)

        # Commit the changes and close the connection
        conn.commit()
        conn.close()

        st.success(f"Data has been edited in the {table_name} table.")


