import pandas as pd  
import numpy as np  
import argparse
import glob
from sklearn.model_selection import train_test_split  
from sklearn.linear_model import LinearRegression 
from sklearn import metrics
import matplotlib.pyplot as plt  

parser = argparse.ArgumentParser()
parser.add_argument("--interval", type=int, default=10, help="Insert interval to integrate data (days)")
args = parser.parse_args()
if args.interval:
    num = args.interval
    data_int = int(num)


dataset_path = glob.glob("./preprocess/newdataset_{}_days_interval/*.csv".format(data_int))
dataset = pd.read_csv(dataset_path[0],names=["timestamp","content_id","counts","count_by_window","d1", "d2", "label"], sep=";")
dataset = dataset.sort_values(by=["timestamp"],ascending=True)

split_point = (int)(dataset.shape[0]*0.9)

X_train = dataset[["count_by_window", "d1", "d2"]].iloc[23:split_point]
y_train = dataset["label"].iloc[23:split_point]
X_test = dataset[["count_by_window", "d1", "d2"]].iloc[split_point:dataset.shape[0]-23]
y_test = dataset["label"].iloc[split_point:dataset.shape[0]-23]

print(dataset["counts"].mean())

#X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.05, random_state=0) 

#dataset_test = pd.read_csv("../dataset_content318_8day_derivative_2/test_data.txt",names=["counts","d1", "d2", "label"], sep=";")

#X_test = dataset_test[["counts", "d1", "d2"]]
#y_test = dataset_test["label"]

regressor = LinearRegression()  
regressor.fit(X_train, y_train)  
coeff_df = pd.DataFrame(regressor.coef_, X_test.columns, columns=['Coefficient'])  
print(coeff_df)  
y_pred = regressor.predict(X_test) 


#df = pd.DataFrame({'Actual': y_test, 'Predicted': y_pred, 'Old_value': X_test.counts})  
df = pd.DataFrame({'Actual': y_test, 'Predicted': y_pred})  
df_percent = pd.DataFrame({"Percentage" : abs(df["Actual"] - df["Predicted"])*100/df["Actual"]})
lines = df.plot.line()
#df_error = pd.DataFrame({"A": abs(df["Old_value"]-df["Actual"]) - abs(df["Predicted"]-df["Actual"])})
#df_error["B"] = pd.DataFrame({"B": abs(df["Predicted"]-df["Actual"])})
#print(df_error["Error"])
#lines = df_error.plot.line()

print("Mean Content Popularity {}".format(df["Actual"].mean()))
print("Mean Error Percentage {}%".format(df_percent["Percentage"].mean()))
print("Mean Absolute Error: {}".format(metrics.mean_absolute_error(y_test, y_pred)))
print("Mean Squared Error: {}".format(metrics.mean_squared_error(y_test, y_pred)))
print("Explained Variance Score: {}".format(metrics.explained_variance_score(y_test, y_pred)))
print("Mean Squared Log Error: {}".format(metrics.mean_squared_log_error(y_test, y_pred))) 
print("Median Absolute Error: {}".format(metrics.median_absolute_error(y_test, y_pred))) 
print("Root Mean Squared Error: {}".format(np.sqrt(metrics.mean_squared_error(y_test, y_pred))))
#print(df_error["A"].mean())
plt.show()