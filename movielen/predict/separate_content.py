import pandas as pd  
import numpy as np  


dataset_all = pd.read_csv("../dataset_5k_10days_interval/dataset_5k_10days_interval.txt",names=["content_id","counts","d1", "d2", "label"], sep=";")

df_grouped = dataset_all.groupby("content_id")

for content_id, group in df_grouped:
    group.to_csv("./data_separated/{}.csv".format(content_id))