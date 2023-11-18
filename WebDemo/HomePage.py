import streamlit as st
st.set_page_config(page_title="Import_file", page_icon="🌐")
# URL của các hình đại diện
csv_icon_url = "https://cdn-icons-png.flaticon.com/512/6133/6133884.png"
text_icon_url = "https://cdn-icons-png.flaticon.com/512/3979/3979306.png"
doc_icon_url = "https://cdn-icons-png.flaticon.com/512/3997/3997559.png"
page_bg_img = """
<style>
[data-testid="stAppViewContainer"] {
    background-image: url("https://accgroup.vn/wp-content/uploads/2023/02/Background-la-gi.jpg.webp");
    background-size: 100% 100%;
}
[data-testid="stHeader"]{
    background: rgba(0,0,0,0);
}
[data-testid="stToolbar"]{
    right:2rem;
}
</style>
"""
st.markdown(page_bg_img, unsafe_allow_html=True)
st.write('<h1 style="color: white; font-size: 40px;">Welcome to my page!</h1>', unsafe_allow_html=True)
# Thay đổi CSS để hiển thị các phần tử trên cùng một hàng ngang
st.markdown(
    """
    <style>
    .row {
        display: flex;
        flex-direction: row;
        justify-content: flex-start;
    }
    .column {
        display: flex;
        align-items: center;
        margin-right: 20px;
    }
    .column img {
        width: 150px;
        margin-right: 10px;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# Hiển thị các hình ảnh trên cùng một hàng ngang
st.markdown(
    """
    <div class="row">
        <div class="column">
            <img src="{}" />
            <p style="color: white; margin: 0;">File CSV</p>
        </div>
        <div class="column">
            <img src="{}" />
            <p style="color: white; margin: 0;">File văn bản</p>
        </div>
        <div class="column">
            <img src="{}" />
            <p style="color: white; margin: 0;">File DOC</p>
        </div>
    </div>
    """.format(csv_icon_url, text_icon_url, doc_icon_url),
    unsafe_allow_html=True
)