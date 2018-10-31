import pandas as pd  
import numpy as np  
import matplotlib.pyplot as plt  
from sklearn.model_selection import train_test_split  
from sklearn.linear_model import LinearRegression 
from sklearn import metrics

timestamp_ = 1776
list_content = pd.read_csv("./content_list/content_list.csv",names=["content_id"], sep=";")

def train_by_content_id(row):
    if row is not None:
        data_train = pd.read_csv("./data_separated/{}_train.csv".format(row["content_id"]),names=["timestamp","content_id","counts","d1", "d2", "label"], sep=";")

        X_train = data_train[["counts", "d1", "d2"]]
        y_train = data_train["label"]

        data_test = pd.read_csv("./data_separated/{}_test.csv".format(row["content_id"]),names=["timestamp","content_id","counts","d1", "d2", "label"], sep=";")

        X_test = data_test[["counts", "d1", "d2"]]
        y_test = data_test["label"]
        if X_test.shape[0] == 0:
            return 0
        regressor = LinearRegression()  
        regressor.fit(X_train, y_train) 
        y_pred = regressor.predict(X_test)
        return y_pred[0]
    else:
        return 0
list_content["counts"] = list_content.apply(lambda row: train_by_content_id(row), axis=1)
list_content.counts = list_content.counts.round()
data_sorted = list_content.sort_values(by=["counts"],ascending=False)
data_sorted = data_sorted[["counts","content_id"]]
data_sorted.to_csv("./content_popularity/timestamp_{}.csv".format(timestamp_), header=False, sep=";", index=False)