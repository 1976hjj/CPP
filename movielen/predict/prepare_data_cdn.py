import pandas as pd  
import numpy as np  

timestamp_ = 1775

dataset_all = pd.read_csv("../dataset_5k_10days_interval/data_10day_interval/dataset_10_days_interval/dataset_10_days_interval.txt",names=["timestamp","content_id","counts","d1", "d2", "label"], sep=";")

df_grouped = dataset_all.groupby("content_id")

for content_id, group in df_grouped:
    group = group.iloc[3:]
    group[group.timestamp < (timestamp_ + 1)].to_csv("./data_separated/{}_train.csv".format(content_id), header=False, sep=";", index=False)
    group[group.timestamp == (timestamp_ + 1)].to_csv("./data_separated/{}_test.csv".format(content_id), header=False, sep=";", index=False)

datalist_all = pd.read_csv("../dataset_5k_10days_interval/data_10day_interval/datalist_10_days_interval/datalist_10_days_interval.txt",names=["timestamp","content_id","counts","const"], sep=";")

data_filtered = datalist_all[datalist_all.timestamp == timestamp_]
data_sorted = data_filtered[["counts", "content_id"]].sort_values(by=["counts"],ascending=False)
data_sorted.to_csv("./content_popularity/timestamp_{}.csv".format(timestamp_), header=False, sep=";", index=False)
list_content_id = pd.read_csv("./content_list/data_sort_by_num_access_5k.csv",names=["content_id","counts","rating"], sep=";")
content_list = list_content_id[["content_id"]].to_csv("./content_list/content_list.csv", header=False, sep=";", index=False)