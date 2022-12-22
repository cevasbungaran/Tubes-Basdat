# -*- coding: utf-8 -*-
"""
Created on Fri Dec 16 13:16:41 2022

@author: cevas
"""

import pandas as pd

#Membersihkan data amazon_jewelry_reviews.csv
amazon_jewelry_reviews = pd.read_csv("amazon_jewelry_reviews.csv")
amazon_jewelry_reviews = amazon_jewelry_reviews.dropna()
amazon_jewelry_reviews.isna()
if amazon_jewelry_reviews.isna().any().any():
    print("Contains NA values")
else:
    print("Does not contain any NA values")

amazon_jewelry_reviews.to_csv("amazon_jewelry_reviews_cleaned.csv", index=False)

#Membersihkan data amazon_toys_reviews.csv
amazon_toys_reviews = pd.read_csv("amazon_toys_reviews.csv")
amazon_toys_reviews = amazon_toys_reviews.dropna()
amazon_toys_reviews.isna()
if amazon_toys_reviews.isna().any().any():
    print("Contains NA values")
else:
    print("Does not contain any NA values")

amazon_toys_reviews.to_csv("amazon_toys_reviews_cleaned.csv", index=False)

#Membersihkan data amazon_shoes_reviews.csv
amazon_shoes_reviews = pd.read_csv("amazon_shoes_reviews.csv")
amazon_shoes_reviews = amazon_shoes_reviews.dropna()
amazon_shoes_reviews.isna()
if amazon_shoes_reviews.isna().any().any():
    print("Contains NA values")
else:
    print("Does not contain any NA values")

amazon_shoes_reviews.to_csv("amazon_shoes_reviews_cleaned.csv", index=False)

#Membersihkan data amazon_books_reviews.csv
amazon_books_reviews = pd.read_csv("amazon_books_reviews.csv")
if amazon_books_reviews.isna().any().any():
    print("Contains NA values")
else:
    print("Does not contain any NA values")    

amazon_books_reviews.to_csv("amazon_books_reviews_cleaned.csv", index=False)

df_cleaned = pd.DataFrame()

#Memasukan dataset csv ke HDFS
from hdfs import InsecureClient
client_hdfs = InsecureClient('http://localhost:9870')



with client_hdfs.write('amazon_books_reviews_cleaned.csv', encoding='utf-8') as writer:
    amazon_books_reviews.to_csv(writer, index = False)
    
with client_hdfs.write('amazon_books_reviews_cleaned.csv', encoding='utf-8') as writer:
    amazon_books_reviews.to_csv(writer, index = False)
    
with client_hdfs.write('amazon_books_reviews_cleaned.csv', encoding='utf-8') as writer:
    amazon_books_reviews.to_csv(writer, index = False)
    
with client_hdfs.write('amazon_books_reviews_cleaned.csv', encoding='utf-8') as writer:
    amazon_books_reviews.to_csv(writer, index = False)


