# -*- coding: utf-8 -*-
"""
Created on Fri Dec 16 13:16:41 2022

@author: cevas
"""

import pandas as pd

#Membersihkan data drug_reviews.csv
drug_reviews = pd.read_csv("drug_reviews.csv")
drug_reviews = drug_reviews.dropna()
drug_reviews.isna()
if drug_reviews.isna().any().any():
    print("Contains NA values")
else:
    print("Does not contain any NA values")

drug_reviews.to_csv("drug_reviews_cleaned.csv", index=False)

#Membersihkan data amazon_books_reviews.csv
amazon_books_reviews = pd.read_csv("amazon_books_reviews.csv")
if amazon_books_reviews.isna().any().any():
    print("Contains NA values")
else:
    print("Does not contain any NA values")    

amazon_books_reviews.to_csv("amazon_books_reviews_cleaned.csv", index=False)

#Memasukan dataset csv ke HDFS
from hdfs import InsecureClient
client_hdfs = InsecureClient('http://localhost:9870')

with client_hdfs.write('drug_reviews_cleaned.csv', encoding='utf-8') as writer:
    drug_reviews.to_csv(writer, index = False)

with client_hdfs.write('amazon_books_reviews_cleaned.csv', encoding='utf-8') as writer:
    amazon_books_reviews.to_csv(writer, index = False)


