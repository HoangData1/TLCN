import streamlit as st
import pandas as pd
import os
from pyspark.sql import SparkSession
import plotly.figure_factory as ff
import plotly.express as px
import matplotlib.pyplot as plt
import plost

def ratings_chart(data):
  st.write("2")
def price_distribute_chart(data):
  st.markdown('### Số lượng sản phẩm dựa theo giá cả')
  plot_bin = st.slider('Độ chi tiết', 20, 1000, 25)
  fig, ax = plt.subplots()
  ax.hist(data['actual_price'], bins=plot_bin)
  st.pyplot(fig)

def number_of_reviews(data):
  st.markdown('### Number of Reviews Distribution')
  fig = px.histogram(data, "no_of_ratings",
                  color_discrete_sequence = ["#8B4000"] * len(data))
  fig.update_xaxes(range=[10, 5000])
  fig.update_yaxes(range=[0, 2000])
  fig.update_layout(
                plot_bgcolor = "#ECECEC",
                xaxis_title = "Number of Reviews",
                )
  st.write(fig)

def number_of_reviews_and_discount_percent(data):
  st.markdown('### Phần trăm giảm giá và số ratings tương ứng')
  st.scatter_chart(
    data,
    x='discounting_percent',
    y='no_of_ratings',
    color='#0068c9',
    size='discounting_percent'
  )

def number_of_reviews_and_price(data):
  st.markdown('### Giá và số ratings tương ứng')
  st.scatter_chart(
    data,
    x='actual_price',
    y='no_of_ratings',
    color='#0068c9',
    size='actual_price'
  )
def ratings_and_price(data):
  st.markdown('### Giá và ratings trung bình')
  st.scatter_chart(
    data,
    x='actual_price',
    y='ratings',
    color='#0068c9',
    size='actual_price'
  )
def show_chart(uploaded_file):
  st.markdown('### Dữ liệu')
  data = pd.read_csv(uploaded_file)
  st.write(data.head(2))
  price_distribute_chart(data)
  # ratings_chart(data,"ratings")
  number_of_reviews_and_discount_percent(data)
  number_of_reviews(data)
  number_of_reviews_and_price(data)
  ratings_and_price(data)

##sidebar
st.set_page_config(layout='wide', initial_sidebar_state='expanded')

mydir = 'C:/Users/Hoang/Desktop/TLCN/WebDemo/pages'
myfile = 'style.css'
training_images_labels_path = os.path.join(mydir, myfile)
style = os.path.join(mydir, myfile)

with open(style) as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    

action_upload = st.sidebar.selectbox("Chọn file hoặc lấy từ database:",["Chọn file hoặc lấy từ database","File","DB"])

if action_upload == "File": 
  #file upload
  uploaded_file = st.sidebar.file_uploader("Chọn một tệp CSV", type=["csv"])
  if uploaded_file is not None:
    show_chart(uploaded_file)


elif action_upload == "DB":
  spark = SparkSession.builder \
    .appName("SimpleApp") \
    .master('spark://ubuntu:7077') \
    .config('spark.executor.cores', '2') \
    .config('spark.executor.instances', '1') \
    .config('spark.driver.cores', '2') \
    .config('spark.cores.max', '2') \
    .config('spark.executor.memory', '512m') \
    .config("spark.hadoop.fs.s3a.access.key", "knSN4neLpvbCAaWedDPS") \
    .config("spark.hadoop.fs.s3a.secret.key","qQEYztGdx0PjdZVkq1GK2zZ1F6btgP3W4EQaXcrk") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
  uploaded_file = spark.read.format("delta").load("s3a://haha/Amazon_Sliver")
  uploaded_file = uploaded_file.toPandas()
  if uploaded_file is not None:
    data = uploaded_file
    price_distribute_chart(data)
    # ratings_chart(data,"ratings")
    number_of_reviews_and_discount_percent(data)
    number_of_reviews(data)
    number_of_reviews_and_price(data)
    ratings_and_price(data)
st.sidebar.markdown('''
---
Created with ❤️ by [Nhóm 2]().
''')
####body



  