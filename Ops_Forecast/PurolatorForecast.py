import pandas as pd
import numpy as np
import sys
import os
import itertools
root_folder  = 'C:\My Folder\Python Projects\General files'
sys.path.insert(0, root_folder)
import functions 
#import plotly.graph_objects as go
#from plotly.subplots import make_subplots
import plotly.io as io
io.renderers.default='browser' # or you can use 'svr'
import plotly.express as px
from sklearn.metrics import mean_absolute_error,mean_absolute_percentage_error,mean_squared_error

from xgboost import XGBRegressor
from sklearn.ensemble import AdaBoostRegressor
from sklearn.tree import DecisionTreeRegressor
#import Prophet


def splitting_train_test(df,length):
    train = df[:df.shape[0]-length]
    test = df[-length:]

    return train,test

def ceating_Xs_Ys(train,test,col):
    X_train = train.drop(['Date']+ [col],axis=1)
    y_train = train[col]


    X_test = test.drop(['Date']+ [col],axis=1)
    y_test = test[col]

    return X_train,y_train,X_test,y_test




""" FORECAST Functions """
import torch.optim as optim
import torch.nn as nn
import copy
import numpy as np
import torch
import tqdm
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt

def NN_for_regression(X_train,y_train,X_test,y_test,test,col):
# Define the model
    n_inputs = X_train.shape[1]
    model = nn.Sequential(
        nn.Linear(n_inputs, 56),
        #nn.ReLU(),
        nn.Linear(56, 24),
        #nn.ReLU(),
        nn.Linear(24, 12),
        nn.ReLU(),
        nn.Linear(12, 8),
        #nn.ReLU(),
        nn.Linear(8, 1)
    )


    # loss function and optimizer
    loss_fn = nn.L1Loss()  # mean square error
    optimizer = optim.Adam(model.parameters(), lr=0.0001)


    # train-test split of the dataset
    #X_train, X_test, y_train, y_test = train_test_split(X, y, train_size=0.7, shuffle=True)
    X_train = torch.tensor(X_train.to_numpy(), dtype=torch.float32)
    y_train = torch.tensor(y_train.to_numpy(), dtype=torch.float32).reshape(-1, 1)
    X_test = torch.tensor(X_test.to_numpy(), dtype=torch.float32)
    y_test = torch.tensor(y_test.to_numpy(), dtype=torch.float32).reshape(-1, 1)

    # training parameters
    n_epochs = 100   # number of epochs to run
    batch_size = 10  # size of each batch
    batch_start = torch.arange(0, len(X_train), batch_size)

    # Hold the best model
    best_mse = np.inf   # init to infinity
    best_weights = None
    history = []

    # training loop
    for epoch in range(n_epochs):
        model.train()
        with tqdm.tqdm(batch_start, unit="batch", mininterval=0, disable=True) as bar:
            bar.set_description(f"Epoch {epoch}")
            for start in bar:
                # take a batch
                X_batch = X_train[start:start+batch_size]
                y_batch = y_train[start:start+batch_size]
                # forward pass
                y_pred = model(X_batch)
                loss = loss_fn(y_pred, y_batch)
                # backward pass
                optimizer.zero_grad()
                loss.backward()
                # update weights
                optimizer.step()
                # print progress
                bar.set_postfix(mse=float(loss))
        # evaluate accuracy at end of each epoch
        model.eval()
        y_pred = model(X_test)
        mse = loss_fn(y_pred, y_test)
        mse = float(mse)
        history.append(mse)
        if mse < best_mse:
            best_mse = mse
            best_weights = copy.deepcopy(model.state_dict())

    # restore model and return best accuracy
    model.load_state_dict(best_weights)

    with torch.inference_mode():
        test_pred = model(X_test)
        test_loss = loss_fn(y_test,test_pred)
    

    print("%.2f Test mae:"% test_loss)
    print("MAE: %.2f" % best_mse)
    print("RMSE: %.2f" % np.sqrt(best_mse))
    #plt.plot(history)
    #plt.show()

    NN_predictions = test.copy()
    NN_predictions [col] = test_pred


    return NN_predictions

def adaboost(X_train,y_train,X_test,y_test,test,col):

    DTR=DecisionTreeRegressor(max_depth=20,random_state=42)
    from xgboost import XGBRegressor
    #DTR =  XGBRegressor()
    
    RegModel = AdaBoostRegressor(n_estimators=70, base_estimator=DTR ,learning_rate=0.01)
    AB=RegModel.fit(X_train,y_train)
    prediction=AB.predict(X_test)
    
    
    ada_prediction = test.copy()
    ada_prediction [col] = prediction

    return ada_prediction

def xgboost(X_train,y_train,X_test,test,col):
    modl = XGBRegressor()
    modl.fit(X_train,y_train)
    pred = modl.predict(X_test)

    predictions = test.copy()
    predictions['Total Del Stops'] = pred
    return predictions

def decision_tree_regressor(X_train,y_train,X_test,test,col):
    # Create a DecisionTreeRegressor model
    regression_tree = DecisionTreeRegressor(random_state=42)  # You can specify the max depth or other hyperparameters

    # Fit the model to your data
    regression_tree.fit(X_train, y_train)  

    # Make predictions
    y_pred = regression_tree.predict(X_test)  

    predictions = test.copy()
    predictions[col] = y_pred

    return predictions

def forecast_prophet(X_train,y_train,col, forecast_periods=365):
    """
    Generate forecasts using Prophet.

    Args:
        time_series_data (pd.DataFrame): Historical time series data with 'ds' (datetime) and 'y' (numeric) columns.
        forecast_periods (int): Number of future periods to forecast.

    Returns:
        pd.DataFrame: Forecasted data with 'ds' (datetime), 'yhat' (predicted values), 'yhat_lower' (lower bound), and 'yhat_upper' (upper bound).
    """
    time_series_data = []
    time_series_data['ds'] = X_train['Date']
    time_series_data['y'] = y_train[col]


    # Initialize Prophet model
    model = Prophet()

    # Fit the model to the historical data
    model.fit(time_series_data)

    # Create a dataframe for future forecasting
    future = model.make_future_dataframe(periods=forecast_periods)

    # Generate forecasts
    forecast = model.predict(future)

    


    return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]

