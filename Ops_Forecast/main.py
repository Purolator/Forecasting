import pandas as pd
import numpy as np
import sys
import os
import itertools
root_folder  = 'C:\My Folder\Python Projects\General files'
sys.path.insert(0, root_folder)
import functions  
import PurolatorForecast as pf
from pandas.tseries.offsets import BDay
import config
from config import dropped_columns,non_variable_columns,variable_columns
import FeatureEngineering as fe
import plotting_charts as pc
import metric
terminal=552


terminal_list = functions.sql_reading_table(f"Select * from terminal_list")


""" Importing data """
fmr = functions.sql_reading_table(f"Select * from fmr where Terminal={terminal}").rename(columns={'CalendarDate':'Date'})
amz = functions.sql_reading_table(f"Select * from amazon where Terminal={terminal}").rename(columns={'Pieces':'Amz Pieces'}).groupby(['Date','Terminal'])['Amz Pieces','Stops'].sum().reset_index()
forecast = pd.read_csv(r'C:\My Folder\Python Projects\Daily Model - PyTorch\Version 1\forecast.csv').rename(columns={'CalendarDate':'Date'})
forecast.columns = forecast.columns.str.replace("Forecast-", '')



""" Pre Processing data """
regressors_columns = ['Amz Pieces']



# FMR DATA
df = pd.merge(fmr,amz[['Date','Terminal','Amz Pieces']],left_on=['Date','Terminal'],right_on=['Date','Terminal'],how='left').drop_duplicates()
fmr_adj = df[df['Date']>="2017-01-01"]
fmr_adj = fmr_adj.fillna(0)
fmr_adj = fmr_adj[['Date'] + regressors_columns + ['Total Del Stops']] # Change this in the general form
prov = terminal_list[terminal_list['Terminal']==terminal]['Province']
fmr_adj = fe.feature_engineering(fmr_adj,prov)

# FORECAST
last_date = fmr.iloc[-1]['Date']
forecast = forecast[forecast['Terminal']==terminal]
forecast['Date'] = pd.to_datetime(forecast['Date'])
forecast = forecast[forecast['Date']<=last_date]


random_state = 42
test_length = 14

""" DATA is ready"""
# Get the list of column names with the object data type (str)
object_columns = fmr_adj.select_dtypes(include=['object']).columns
# Drop the columns with the object data type
fmr_adj = fmr_adj.drop(object_columns, axis=1)
fmr_adj         = fmr_adj.dropna()


""" TRAIN TEST SPLIT """
[train,test]                        = pf.splitting_train_test(fmr_adj,length=test_length)
[X_train,y_train,X_test,y_test]     = pf.ceating_Xs_Ys(train,test,col = 'Total Del Stops' )
[forecast_train,forecast_test]      = pf.splitting_train_test(forecast,length=test_length)

""" PREDICTION PART """
pred_xgb        = pf.xgboost(X_train,y_train,X_test,test,col = 'Total Del Stops')
pred_nn         = pf.NN_for_regression(X_train,y_train,X_test,y_test,test,col = 'Total Del Stops')
pred_ada        = pf.adaboost(X_train,y_train,X_test,y_test,test,col = 'Total Del Stops')
pred_dtr        = pf.decision_tree_regressor(X_train,y_train,X_test,test,col= 'Total Del Stops')
#pr_pred         = pf.forecast_prophet(X_train,y_train,col, forecast_periods=365)

""" PLOT RESULT """
Methods         = ['OpsForecast'] + ['Xgboost','Neural Network','ADAboost','DTRegressor']
dataframe_names = ['OpsForecast'] + ['plot_historical','Test','Xgboost','Neural Network','ADAboost','DTRegressor']
[plot_ops_forecast,plot_historical,plot_test_chart,plot_xgboost,plot_nn,plot_ada,plot_dtr] = pc.make_df_ready_for_plotting(forecast_test,fmr_adj,test,pred_xgb,pred_nn,pred_ada,pred_dtr,table_names = dataframe_names)
pc.draw_line_charts(plot_ops_forecast,plot_historical[-50:],plot_test_chart,plot_xgboost,plot_nn,plot_ada,plot_dtr,table_names=dataframe_names)
MAE = metric.calculate_mae(plot_ops_forecast,pred_xgb,pred_nn,pred_ada,pred_dtr,y_test=y_test,methods=Methods)
print(MAE)


