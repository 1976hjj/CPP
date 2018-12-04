import pandas as pd  
import numpy as np  
import argparse
import glob
from sklearn.model_selection import train_test_split  
from sklearn.linear_model import LinearRegression 
from sklearn import metrics

def toInt(num):
    try:
        return int(num)
    except ValueError:
        return None
def toFloat(num):
    try:
        return float(num)
    except ValueError:
        return None

# Parse data interval  (default interval: 5 days)
parser = argparse.ArgumentParser()
parser.add_argument("--timestamp", help="Insert interval to integrate data (days)")
parser.add_argument("--interval", help="Insert interval to integrate data (days)")
args = parser.parse_args()
data_timestamp = 1775 #default value
if args.timestamp:
    num = args.timestamp
    if toInt(num) is not None:
        data_timestamp = int(num)
data_int = 10 #default value
if args.interval:
    num = args.interval
    if toInt(num) is not None:
        data_int = int(num)

dataset_path = glob.glob("./preprocess/dataset_{}_days_interval/*.csv".format(data_int))
dataset_all = pd.read_csv(dataset_path[0],names=["timestamp","content_id","counts","d1", "d2", "label"], sep=";")

df_grouped = dataset_all.groupby("content_id")

for content_id, group in df_grouped:
    group = group.iloc[3:]
    group[group.timestamp < (data_timestamp + 1)].to_csv("./result/data_separated/{}_train.csv".format(content_id), header=False, sep=";", index=False)
    group[group.timestamp == (data_timestamp + 1)].to_csv("./result/data_separated/{}_test.csv".format(content_id), header=False, sep=";", index=False)

datalist_path = glob.glob("./preprocess/datalist_{}_days_interval/*.csv".format(data_int))
datalist_all = pd.read_csv(datalist_path[0],names=["timestamp","content_id","counts","const"], sep=";")

data_filtered = datalist_all[datalist_all.timestamp == data_timestamp]
data_sorted = data_filtered[["counts", "content_id"]].sort_values(by=["counts"],ascending=False)
data_sorted.to_csv("./result/content_popularity/timestamp_{}.csv".format(data_timestamp), header=False, sep=";", index=False)

content_popularity_path = glob.glob("./content_popularity_all/*.csv".format(data_int))
list_content = pd.read_csv(content_popularity_path[0],names=["counts","content_id"], sep=";")
list_content_id = list_content[["content_id"]]

timestamp_ = data_timestamp+1

def train_by_content_id(row):
    if row is not None:
        data_train = pd.read_csv("./result/data_separated/{}_train.csv".format(row["content_id"]),names=["timestamp","content_id","counts","d1", "d2", "label"], sep=";")

        X_train = data_train[["counts", "d1", "d2"]]
        y_train = data_train["label"]
        if X_train.shape[0] < 2:
            return 0

        data_test = pd.read_csv("./result/data_separated/{}_test.csv".format(row["content_id"]),names=["timestamp","content_id","counts","d1", "d2", "label"], sep=";")

        X_test = data_test[["counts", "d1", "d2"]]
        y_test = data_test["label"]
        if X_test.shape[0] < 1:
            return 0
        regressor = LinearRegression()  
        regressor.fit(X_train, y_train) 
        y_pred = regressor.predict(X_test)
        if (y_pred[0] - X_test.iloc[0]["counts"] > 50):
            return X_test.iloc[0]["counts"] + 50
        elif (X_test.iloc[0]["counts"] - y_pred[0] > 50):
            return X_test.iloc[0]["counts"] - 50
        return y_pred[0]
    else:
        return 0
list_content_id["counts"] = list_content_id.apply(lambda row: train_by_content_id(row), axis=1)
list_content_id["counts"] = list_content_id["counts"].astype(int)
list_content_id = list_content_id[list_content_id["counts"] > 0]
data_sorted_pre = list_content_id.sort_values(by=["counts"],ascending=False)
data_sorted_pre = data_sorted_pre[["counts","content_id"]]
data_sorted_pre.to_csv("./result/content_popularity/timestamp_{}.csv".format(timestamp_), header=False, sep=";", index=False)

df_all_content = pd.merge(data_sorted_pre, data_sorted, on="content_id", how="outer").fillna(0)

dataset_request_path = glob.glob("./preprocess/content_list_{}_days_interval/*.csv".format(data_int))
dataset_request = pd.read_csv(dataset_request_path[0],names=["content_id", "counter"], sep=";")

df_all_content = pd.merge(df_all_content, dataset_request, on="content_id", how="outer").fillna(0).sort_values(by=["content_id"],ascending=False)
df_all_content["new_id"] = range(1,len(df_all_content)+1)
df_all_content.round(0).astype(int).to_csv("./result/content_popularity/all_content_{}.csv".format(timestamp_), header=False, sep=";", index=False)

# Generate request
dataset_all_path = glob.glob("./preprocess/datacache_indexed_{}_days_interval/*.csv".format(data_int))
dataset_all = pd.read_csv(dataset_all_path[0],names=["timestamp", "content_id", "timestamp_", "cache"], sep=";")

df_grouped = dataset_all.groupby("cache")
for cache, group in df_grouped:
    group[["content_id"]].to_csv("./result/cache/Cache_{}.csv".format(cache+1), header=False, sep=";", index=False)
