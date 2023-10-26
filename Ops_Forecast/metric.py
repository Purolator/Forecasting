
import pandas as pd
import numpy as np
import sys
from sklearn.metrics import mean_absolute_error,mean_absolute_percentage_error,mean_squared_error



#########################################################################################################
#########################################################################################################
""" METRIC Functions """
def calculate_mae(*predictors,y_test,methods):
    result_data = []
    for idx, pred in enumerate(predictors):
        mae = mean_absolute_error(y_test, pred["Total Del Stops"])
        mape = mean_absolute_percentage_error(y_test, pred["Total Del Stops"])
        mse  = mean_squared_error(y_test, pred["Total Del Stops"])
        input_name = methods[idx]
        result_data.append({"Input": input_name, "MAE": mae, "MAPE":mape, "MSE":mse})
    
    result_dataframe = pd.DataFrame(result_data)
    result_dataframe = result_dataframe.sort_values(["MAE"])
    return result_dataframe