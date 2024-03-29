import pandas as pd  
import numpy as np  
import argparse
import schema
import glob


# Parse data interval  (default interval: 5 days)
parser = argparse.ArgumentParser()
parser.add_argument("--timestamp", help="Insert interval to integrate data (days)")
args = parser.parse_args()
data_timestamp = lit(1775) #default value
if args.timestamp:
    num = args.timestamp
    if schema.toInt(num) is not None:
        data_timestamp = int(num)
parser.add_argument("--interval", help="Insert interval to integrate data (days)")
args = parser.parse_args()
data_int = lit(10) #default value
if args.interval:
    num = args.interval
    if schema.toInt(num) is not None:
        data_int = int(num)

dataset_path = glob("./dataset_{}_days_interval/*.csv".format(data_int))
dataset_all = pd.read_csv(dataset_path[0],names=["timestamp","content_id","counts","d1", "d2", "label"], sep=";")

df_grouped = dataset_all.groupby("content_id")

for content_id, group in df_grouped:
    group = group.iloc[3:]
    group[group.timestamp < (data_timestamp + 1)].to_csv("./data_separated/{}_train.csv".format(content_id), header=False, sep=";", index=False)
    group[group.timestamp == (data_timestamp + 1)].to_csv("./data_separated/{}_test.csv".format(content_id), header=False, sep=";", index=False)

datalist_path = glob("./datalist_{}_days_interval/*.csv".format(data_int))
datalist_all = pd.read_csv(datalist_path[0],names=["timestamp","content_id","counts","const"], sep=";")

data_filtered = datalist_all[datalist_all.timestamp == data_timestamp]
data_sorted = data_filtered[["counts", "content_id"]].sort_values(by=["counts"],ascending=False)
data_sorted.to_csv("./content_popularity/timestamp_{}.csv".format(data_timestamp), header=False, sep=";", index=False)

content_popularity_path = glob("./content_popularity_all/*.csv".format(data_int))
list_content = pd.read_csv(content_popularity_path[0],names=["counts","content_id"], sep=";")
list_content_id = list_content[["content_id"]]

timestamp_ = data_timestamp+1

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
list_content_id["counts"] = list_content_id.apply(lambda row: train_by_content_id(row), axis=1)
list_content_id.counts = list_content_id.counts.round()
data_sorted = list_content_id.sort_values(by=["counts"],ascending=False)
data_sorted = data_sorted[["counts","content_id"]]
data_sorted.to_csv("./content_popularity/timestamp_{}.csv".format(timestamp_), header=False, sep=";", index=False)


dataset_all_path = glob("./dataround_{}_days_interval/*.csv".format(data_int))
dataset_all = pd.read_csv(dataset_all_path[0],names=["timestamp", "content_id", "counter", "timestamp_"], sep=";")
dataset_filter = dataset_all[dataset_all.timestamp == (data_timestamp + 1)].sort_values(by=["timestamp_"],ascending=True).reset_index(inplace=True)
dataset_filter["cache"] = (dataset_filter["Index"] % 55) + 1
df_grouped = dataset_filter.groupby("cache")

for cache, group in df_grouped:
    group[["content_id"]].to_csv("./cache/Cache_{}.csv".format(cache), header=False, sep=";", index=False)
