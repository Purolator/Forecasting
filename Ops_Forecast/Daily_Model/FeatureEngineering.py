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
import holidays
from pandas.tseries.offsets import BDay



def feature_engineering(df,prov):
    """_summary_

    Args:
        df (dataframe):   df has to have at least two columns "Date|variable(Total Del Stops)"
    Returns:
        _type_: Adding different features like Year,Week,Wday,Month,Weekend,...
    """

    # Filling missing dates
    df = adding_missing_dates(df) 
    df = adding_natiopnal_holiday(df)
    df = adding_provincial_holiday(df,prov)
    df = creating_date_components(df)
    df = adding_rolling_features(df)
    df = creating_lagged_holidays_impact(df)


    return df 



def adding_natiopnal_holiday(df):
    national_holiday_list = pd.read_excel('holiday.xlsx',sheet_name="National_holidays")
    df = pd.merge(df,national_holiday_list,on='Date',how='left')
    df['IsHoliday'] = np.where(df['HolidayName'].isna(),0,1)

    from sklearn.preprocessing import LabelEncoder
    # Create a LabelEncoder instance
    label_encoder = LabelEncoder()

    # Encode the column 'category' in your DataFrame
    df['Encoded_HolidayName'] = label_encoder.fit_transform(df['HolidayName'])

    return df


def adding_missing_dates(df):

    # Convert the 'date' column to a datetime object
    df['Date'] = pd.to_datetime(df['Date'])

    # Create a date range covering the entire date range in the DataFrame
    min_date = df['Date'].min()
    max_date = df['Date'].max()
    date_range = pd.date_range(start=min_date, end=max_date, freq='D')

    # Create a DataFrame with the full date range
    full_df = pd.DataFrame({'Date': date_range})

    # Merge the full date range DataFrame with the original DataFrame
    merged_df = full_df.merge(df, on='Date', how='left')

    # Fill missing values with 0 in the 'Total Del Stops' column
    merged_df.fillna(0, inplace=True)

    return merged_df


def creating_date_components(df):

    df['Year'] = df['Date'].dt.year
    df['Week'] = df['Date'].dt.week
    df['Wday'] = df['Date'].dt.weekday
    df['Month'] = df['Date'].dt.month
    df['Quarter'] =df['Date'].dt.quarter
    df['IsWeekend'] = (df['Date'].dt.dayofweek >= 5).astype(int)
    df['Day_of_Month'] = df['Date'].dt.day

    return df

def adding_rolling_features(df):
    #df['mean_7'] = 0
    #df['mean_30'] = 0
    for i in df['Wday'].unique():
        df.loc[df['Wday']==i,'mean_2'] = df[df['Wday']==i]['Total Del Stops'].rolling(2).mean().shift(7)
        #df.loc[df['Wday']==i,'mean_7'] = df[df['Wday']==i]['Total Del Stops'].rolling(7).mean().shift(7)
        #df.loc[df['Wday']==i,'mean_30'] = df[df['Wday']==i]['Total Del Stops'].rolling(30).mean().shift(7)

    return df


def creating_lagged_holidays_impact(df):
    df['Yesterday_holiday'] = (df['Date'] - BDay(1)).isin(list(df[df['IsHoliday']==1]['Date']))
    df['Yesterday_holiday'] = np.where(df['Yesterday_holiday']==True,1,0)
    df.loc[df['IsWeekend']==1,'Yesterday_holiday'] = 0


    df['Two_days_ago_holiday'] = (df['Date'] - BDay(2)).isin(list(df[df['IsHoliday']==1]['Date']))
    df['Two_days_ago_holiday'] = np.where(df['Two_days_ago_holiday']==True,1,0)
    df.loc[df['IsWeekend']==1,'Two_days_ago_holiday'] = 0

    df['Three_days_ago_holiday'] = (df['Date'] - BDay(3)).isin(list(df[df['IsHoliday']==1]['Date']))
    df['Three_days_ago_holiday'] = np.where(df['Three_days_ago_holiday']==True,1,0)
    df.loc[df['IsWeekend']==1,'Three_days_ago_holiday'] = 0

    return df


def adding_provincial_holiday(df,prov):
    df['Province'] = prov
    provincial_holiday_list = pd.read_excel('holiday.xlsx',sheet_name="Provincial_holidays")
    df = pd.merge(df,provincial_holiday_list,on='Date',how='left')
    df['IsProvincialHoliday'] = np.where(df['ProvincialHolidayName'].isna(),0,1)

    from sklearn.preprocessing import LabelEncoder
    # Create a LabelEncoder instance
    label_encoder = LabelEncoder()

    # Encode the column 'category' in your DataFrame
    df['Encoded_ProvincialHolidayName'] = label_encoder.fit_transform(df['ProvincialHolidayName'])

    return df



 
