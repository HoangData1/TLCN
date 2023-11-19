import streamlit as st
import pandas as pd
import os
from pyspark.sql import SparkSession
import plotly.figure_factory as ff
import plotly.express as px
#import plost


def ratings_chart(data,columns):
  fig = px.violin(data, columns, 
               color_discrete_sequence = ["#FFBF00"] * len(data))
  fig.update_layout(
                plot_bgcolor = "#ECECEC",
                  xaxis_title = "Rating",
                  title = "<b>Rating Distribution of the Popular Products</b>"
                )
  st.plotly_chart(fig)

def price_distribute_chart(data,columns):
  x = data[columns]
  hist_data = [x]
  group_labels = [columns]

  fig = ff.create_distplot(hist_data, group_labels, show_rug = False,
                          colors=["#ffd514"])
  fig.update_layout(
                    plot_bgcolor = "#ECECEC",
                      title = "<b>Price Distribution of Data</b>"
                    )
  st.plotly_chart(fig)

def number_of_reviews(data,columns):
  fig = px.histogram(data, columns,
                  color_discrete_sequence = ["#8B4000"] * len(data))
  fig.update_xaxes(range=[10, 5000])
  fig.update_yaxes(range=[0, 2000])
  fig.update_layout(
                  plot_bgcolor = "#ECECEC",
                    xaxis_title = "Number of Reviews",
                    title = "<b>Number of Reviews Distribution</b>"
                  )
  st.plotly_chart(fig)

st.set_page_config(layout='wide', initial_sidebar_state='expanded')

mydir = 'C:/Users/Lenovo/Desktop/TLCN/TLCN/WebDemo/pages'
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
    action = st.sidebar.selectbox("Choose a colums:", ["Chose a columns to analysis","actual_price", "ratings", "no_of_ratings(number of review)"])
    if action == "actual_price":
      data = pd.read_csv(uploaded_file)
      st.write("Dữ liệu từ tệp CSV:")
      st.write(data)
      price_distribute_chart(data,"actual_price")
    elif action == "ratings":
      data = pd.read_csv(uploaded_file)
      st.write("Dữ liệu từ tệp CSV:")
      st.write(data)
      ratings_chart(data,"ratings")
    elif action == "no_of_ratings(number of review)":
      data = pd.read_csv(uploaded_file)
      st.write("Dữ liệu từ tệp CSV:")
      st.write(data)
      number_of_reviews(data,"no_of_ratings")


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
    action = st.sidebar.selectbox("Choose a colums:", ["Chose a columns to analysis","actual_price", "ratings", "no_of_ratings(number of review)"])
    if action == "actual_price":
      data = uploaded_file
      st.write("Dữ liệu từ DB:")
      st.write(data)
      price_distribute_chart(data,"actual_price")
    elif action == "ratings":
      data = uploaded_file
      st.write("Dữ liệu từ DB:")
      st.write(data)
      ratings_chart(data,"ratings")
    elif action == "no_of_ratings(number of review)":
      data = uploaded_file
      st.write("Dữ liệu từ DB:")
      st.write(data)
      number_of_reviews(data,"no_of_ratings")

st.sidebar.markdown('''
---
Created with ❤️ by [Nhóm 2]().
''')

