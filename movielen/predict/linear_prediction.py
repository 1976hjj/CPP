import pandas as pd  
import numpy as np  
import matplotlib.pyplot as plt  
from sklearn.model_selection import train_test_split  
from sklearn.linear_model import LinearRegression 
from sklearn import metrics 


dataset = pd.read_csv("../dataset_content737_8day_derivative_2/dataset_content737_8day_derivative_2.txt",names=["counts","d1", "d2", "label"], sep=";")

X_train = dataset[["counts", "d1", "d2"]]
y_train = dataset["label"]

print(dataset["counts"].mean())

#X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.05, random_state=0) 

dataset_test = pd.read_csv("../dataset_content737_8day_derivative_2/test_data.txt",names=["counts","d1", "d2", "label"], sep=";")

X_test = dataset_test[["counts", "d1", "d2"]]
y_test = dataset_test["label"]

regressor = LinearRegression()  
regressor.fit(X_train, y_train)  
coeff_df = pd.DataFrame(regressor.coef_, X_test.columns, columns=['Coefficient'])  
print(coeff_df)  
y_pred = regressor.predict(X_test) 


df = pd.DataFrame({'Actual': y_test, 'Predicted': y_pred})  
df_percent = pd.DataFrame({"Percentage" : abs(df["Actual"] - df["Predicted"])*100/df["Actual"]})
lines = df.plot.line()

print("Mean Content Popularity {}".format(df["Actual"].mean()))
print("Mean Error Percentage {}%".format(df_percent["Percentage"].mean()))
print("Mean Absolute Error: {}".format(metrics.mean_absolute_error(y_test, y_pred)))
print("Mean Squared Error: {}".format(metrics.mean_squared_error(y_test, y_pred)))
print("Explained Variance Score: {}".format(metrics.explained_variance_score(y_test, y_pred)))
print("Mean Squared Log Error: {}".format(metrics.mean_squared_log_error(y_test, y_pred))) 
print("Median Absolute Error: {}".format(metrics.median_absolute_error(y_test, y_pred))) 
print("Root Mean Squared Error: {}".format(np.sqrt(metrics.mean_squared_error(y_test, y_pred))))
plt.show()