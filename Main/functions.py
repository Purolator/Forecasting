# -*- coding: utf-8 -*-
"""
Created on Wed Jan 18 12:46:12 2023

@author: Mehrdad.Dadgar
"""

""" Preprocessing Functions"""
import os
import pandas as pd
import numpy as np
from datetime import datetime

import datetime as dt
#import plotly.graph_objects as go
#from plotly.subplots import make_subplots
import plotly.io as io
io.renderers.default='browser' # or you can use 'svr'
import plotly.express as px
#import pyarrow.parquet as pq
from pmdarima import auto_arima,ARIMA # for determining ARIMA orders
from statsmodels.tsa.api import STLForecast,ARIMA
import mysql.connector as connection
import pymysql
import calendar
from sklearn.metrics import mean_absolute_error,mean_absolute_percentage_error
from statsmodels.graphics.tsaplots import plot_acf
from statsmodels.graphics.tsaplots import plot_pacf
import matplotlib.pyplot as plt
from sqlalchemy import create_engine,text
database_url = 'mysql+pymysql://root:123456@localhost/forecasting_schema'

##########################################################################################################
#Fix Python Relative Imports and Auto-completion in VSCode
"""  update this section to your computer  """
dir_folder = "C:\My Folder\Python Projects\General files"
"""
You need two files:
    'DailyHolidayData.parquet''
    'HolidayData.parquet'
"""
# This section is for Ops forecast
connection_type2 = pymysql.connect(host='localhost',
                             user='root',        # update this one
                             password='123456',  # update this one
                             db='forecasting_schema')
cursor = connection_type2.cursor()

mydb = connection.connect(host="localhost", database = 'forecasting_schema',user="root", passwd="123456",use_pure=True,auth_plugin='mysql_native_password')

##########################################################################################################

def preprocessing_data(df,start,finish,Terminal_list,customer=False,terminal=False):
    df = changing_column_names(df)
    print('Changing column names .................. done!')
    df = adding_year_week(df)
    print('adding year and week to data .................. done!')

    df = adding_Workingdays_province(df,Terminal_list)
    df = defining_period(df,start,finish)
    
    if customer!=False : df = df[df['Customer']==customer]
    if terminal!=False : df = df[df['Terminal']==terminal]
    
    print('Normalizing data .................. done!')

    df_weekly_normalized, df_weekly = normalize_weekly_data(df) 
    
    return df_weekly_normalized,df_weekly


def changing_column_names(df):
    """ Changing column names and type of columns """
    
    if 'Date' in df.columns:
        df = df[df['Date']>="01-01-2012"]
    
    
    for i in df.columns:
        if i.lower() in ['calendardate','date','calendar date','induction date','edd','eid',
                         'expected delivery date','stopeventdate','expected induction date','podeventdate']:
            df = df.rename(columns={i:'Date'})
            if df['Date'].dtypes in ['datetime64[ns]','datetime64[ns, UTC]'] :
                df['Date'] = pd.to_datetime(df['Date'])
                if df['Date'].dtypes == 'datetime64[ns, UTC]':
                    df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None)
            else:
                df = preprocessing_converting_ordinaldates(df)
            #df.set_index('Date',inplace=True)
        
        if i.lower() in ['week number','week','weeknumber']:
            df = df.rename(columns={i:'Week'})
            df['Week'] = df['Week'].astype(int)
            
        if i.lower() in ['year'] :
            df = df.rename(columns={i:'Year'})
            df['Year'] = df['Year'].astype(int)
        
        if i.lower() in ['customer','master client']:
            df = df.rename(columns={i:'Customer'})
            
        
        if i.lower() in ['termnr','origindepotid',"terminal",'destdepotid','Terminal','stopeventterminal','podeventterminal']:
            df = df.rename(columns={i:'Terminal'})


        if i.lower() in ['prov','province']:
            df = df.rename(columns={i:'Province'})


    if ('Date' in df.columns) and ('Terminal' in df.columns):
        df = df.sort_values(['Date','Terminal'])   
    

    return df


def adding_year_week(df):
    DailyHolidayData=pd.read_parquet(os.path.join(dir_folder,'DailyHolidayData.parquet'))
    df= df.merge(DailyHolidayData[['Date','Year','Week']])
        
    return df


def adding_Workingdays_province(df,Terminal_list):
        
    #Terminal_list = pd.read_excel(os.path.join(dir_folder, "TerminalProvinces.xlsx"))
    Terminal_list = changing_column_names(Terminal_list)
    df = df.merge(Terminal_list,on='Terminal',how='left')

    HolidayData=pd.read_parquet(os.path.join(dir_folder,'HolidayData.parquet'))
    HolidayData = changing_column_names(HolidayData)
    df = df.merge(HolidayData[['Year','Week','Province','Working Days']], how='left',on = ['Year','Week','Province'])

    return df



def defining_period(df,start,finish):
    ''' Defining Start and End of historical data '''
    df = df.loc[( df['Date']>= start) & ( df['Date']<= finish)]
    
    return df


 
def converting_year_week_to_date(df):
    df['Date'] = df['Year-Week'].apply( lambda x : datetime.strptime(x + '-1', "%Y-%W-%w"))

    return df
   

def normalize_weekly_data(df):
    '''Daily dataframe will be given to the function, it will give us normalized weekly and weekly datasets'''

    df_weekly = df.groupby(['Year','Week','Customer','Terminal','Province','Working Days'])['Pieces'].sum().reset_index()
    
    
    df_weekly['Year-Week'] = df_weekly['Year'].astype(str) + "-" + df_weekly['Week'].astype(str)
    df_weekly = converting_year_week_to_date(df_weekly)

    
    
    df_weekly_normalized = df_weekly.copy()
    df_weekly_normalized['Normalized Pieces'] =  (df_weekly_normalized['Pieces']/df_weekly_normalized['Working Days'])*5
    df_weekly_normalized = df_weekly_normalized[['Date','Year-Week','Customer','Terminal','Normalized Pieces','Working Days','Year','Week']]
    df_weekly_normalized = df_weekly_normalized.rename(columns={'Normalized Pieces':'Pieces'})
    
    
    
    df_weekly_normalized = df_weekly_normalized[['Date','Year-Week','Customer','Terminal','Pieces','Working Days','Year','Week']].set_index('Date')
    df_weekly.set_index('Date',inplace=True)
    return df_weekly_normalized,df_weekly


def filling_missing_values(df,start,finish):

    """ This is going to work for weekly data"""
    year_week = pd.DataFrame(pd.read_parquet(os.path.join(dir_folder,'HolidayData.parquet'),columns=['id']).drop_duplicates())
    year_week.columns =["Year-Week"]
    year_week['Date'] = year_week['Year-Week'].apply( lambda x : datetime.strptime(x + '-1', "%Y-%W-%w"))
    
    year_week.set_index('Date',inplace=True)
    year_week['Pieces']=0
    year_week = year_week.drop('Year-Week',axis=1)    
    
    
    df_without_missing = pd.merge(df,year_week, how='right', left_index=True, right_index=True)
    df_without_missing.drop('Pieces_y',axis=1,inplace=True)
    df_without_missing = df_without_missing[(df_without_missing.index>=start)    & (df_without_missing.index<=finish)]
    df_without_missing = df_without_missing.fillna(0)    
    df_without_missing = df_without_missing.rename(columns={'Pieces_x':'Pieces'})

    return df_without_missing


def filling_missing_dates(df):
    df = df.sort_values(by='Date')
    start = df['Date'].to_list()[0]
    end = df['Date'].to_list()[-1]
    all_dates = pd.DataFrame(pd.date_range(start,end), columns=['Date'])
    
    return df

def slicing_customer_terminal(df,customer,terminal):
    """ Slicing based on specific customer and terminal """
    if customer ==None and terminal != None:
        df = df.loc[(df['Terminal']==terminal)]
    if customer != None and terminal != None:
        df = df.loc[(df['Customer']==customer) & (df['Terminal']==terminal)]
    if customer !=None and terminal == None :
        df = df.loc[(df['Customer']==customer)]
    return df
     
def returning_weeknumber(date):
    #date = datetime.strptime(date,'%m-%d-%Y')
    DailyHolidayData=pd.read_parquet(os.path.join(dir_folder,'DailyHolidayData.parquet'))
    year = int(DailyHolidayData[DailyHolidayData['Date']==date]['Year'])
    week = int(DailyHolidayData[DailyHolidayData['Date']==date]['Week'])
    
    return year,week
    
    
def plot_weekly_yoy(df,year='Year',week='Week',type='Type'):    
    
    
    '''  X-axis is week numbers and Y-axis forecast and actuals        '''

    fig = px.line(df, x= week, y='Pieces', color=year,
                      markers=True,
                      line_dash =type,
                      color_discrete_sequence=px.colors.qualitative.G10,line_shape='linear', 
                      color_discrete_map={
                        "2019": "red",
                        "2020": "green",
                        "2021": "blue",
                        "2022": "goldenrod",
                        "2023": "magenta"},
                        title=" Terminal {} - Customer {} ".format(df.Terminal.unique(),df.Customer.unique()),
                        symbol=type)
    fig.show()
    


def graph_checkings(df):
    df.loc[(df['Pieces']<=-0.00001) & (df['Pieces']>=-7),'Pieces'] = 0
    
    return df



def denormalize_data(df,Terminal_list):
    df = adding_Workingdays_province(df,Terminal_list)
    df['Denormalized Pieces'] = np.round((df['Pieces'] /5) * df['Working Days'])
    df = df[['Year','Week','Customer','Terminal','Denormalized Pieces','Type']].rename(columns={'Denormalized Pieces':'Pieces'})
    return df




def appending_actuals_forecast(actual,forecast,forecast_method,customer = None,terminal=None, finish=None):
    actual ['Type'] = 'Actual'
    
    end_year=  returning_weeknumber(finish)[0]
    end_week = returning_weeknumber(finish)[1]
    forecast = pd.DataFrame(forecast).reset_index()
    forecast.columns = ['Date','Pieces']
    forecast.set_index('Date',inplace=True)
    forecast ['Type'] = forecast_method
    
    forecast['Week'] =  np.arange(end_week + 1 , end_week + len(forecast) + 1).tolist()
    forecast['Year'] = np.where(forecast['Week']<=52,end_year,end_year+1).tolist()
    forecast['Week']  = np.where(forecast['Week']>52,forecast['Week']-52, forecast['Week'])
    forecast ['Terminal'] = terminal
    forecast['Customer'] = customer
    
    actual[['Year','Week']] = actual['Year-Week'].str.split("-",expand=True)
    actual['Year'] = actual['Year'].astype(int)
    actual['Week'] = actual['Week'].astype(int)
    columns = ['Year','Week','Customer','Terminal','Pieces','Type']
    output = pd.DataFrame(actual[columns]).append(forecast[columns])

    output['Pieces'] = output['Pieces'].astype(int)
    return output

def appending_newforecast_to_actualforecast_table(df,forecast_values,forecast_method,customer = None,terminal=None, finish=None):
    
    
    end_year=  returning_weeknumber(finish)[0]
    end_week = returning_weeknumber(finish)[1]
    forecast = pd.DataFrame(forecast_values).reset_index()
    forecast.columns = ['Date','Pieces']
    forecast.set_index('Date',inplace=True)
    forecast ['Type'] = forecast_method
    
    forecast['Week'] =  np.arange(end_week + 1 , end_week + len(forecast) + 1).tolist()
    forecast['Year'] = np.where(forecast['Week']<=52,end_year,end_year+1).tolist()
    forecast['Week']  = np.where(forecast['Week']>52,forecast['Week']-52, forecast['Week'])
    forecast ['Terminal'] = terminal
    forecast['Customer'] = customer
    

    columns = ['Year','Week','Customer','Terminal','Pieces','Type']
    df = pd.DataFrame(df[columns]).append(forecast[columns])

 
    return df



def plots_acf_pacf(df):

    fig = plt.figure(figsize=(10,8))
    ax1 = fig.add_subplot(211)
    fig=plot_acf(df,  lags=52, ax=ax1)

    ax2 = fig.add_subplot(212)
    fig=plot_pacf(df, lags=52,  ax=ax2)

    plt.xlabel('Lag')
    plt.suptitle('ACF and PACF plots for Initial Time Series', fontsize=16)
    plt.show()
    
def amazon_actual_forecast(start,finish,terminal_list):   

    amazon_actuals = sql_reading_table("select * from amazon")\
                        [['Date','Terminal','Tier2','Pieces']]\
                        .groupby(['Date','Terminal'])['Pieces']\
                        .sum().reset_index()
    amazon_actuals['Customer'] = 'Amazon'

    amazon_a_weekly_normalized, amazon_a_weekly =preprocessing_data(amazon_actuals, 
                        start=start, finish = finish,Terminal_list = terminal_list)


    amazon_forecast =sql_reading_table("select * from amazon_forecast")\
                        [['Date','Terminal','Pieces']]\
                        .groupby(['Date','Terminal'])['Pieces']\
                        .sum().reset_index()

    amazon_forecast['Customer'] = 'Amazon'

    amazon_f_weekly_normalized, amazon_f_weekly =preprocessing_data(amazon_forecast, 
                                    start=finish+dt.timedelta(1), finish = pd.to_datetime("12-30-2023"),Terminal_list = terminal_list)

    return amazon_a_weekly_normalized,amazon_a_weekly,amazon_f_weekly_normalized,amazon_f_weekly





def weekly_regressor(actual,forecast,terminal,start,finish,customer):
    reg = slicing_customer_terminal(actual,customer=customer,terminal=terminal)
    reg = reg[['Pieces']]
    reg = filling_missing_values(reg,start,finish)

    new_reg = slicing_customer_terminal(forecast,customer=customer,terminal=terminal)
    new_reg = new_reg[['Pieces']]
    new_reg = filling_missing_values(new_reg,start =finish+dt.timedelta(1),finish = pd.to_datetime("12-30-2023"))

    return reg,new_reg


def edd_actual_forecast(start,finish,terminal_list):   

    edd_actuals = sql_reading_table("select * from edd")\
                        [['Date','Terminal','Pieces']]\
                        .groupby(['Date','Terminal'])['Pieces']\
                        .sum().reset_index()
    edd_actuals['Customer'] = 'Edd'

    edd_a_weekly_normalized, edd_a_weekly =preprocessing_data(edd_actuals, 
                        start=start, finish = finish,Terminal_list = terminal_list)


    edd_forecast =sql_reading_table("select * from edd_forecast")\
                        [['Date','Terminal','Pieces']]\
                        .groupby(['Date','Terminal'])['Pieces']\
                        .sum().reset_index()

    edd_forecast['Customer'] = 'Edd'

    edd_f_weekly_normalized, edd_f_weekly =preprocessing_data(edd_forecast, 
                                    start=finish+dt.timedelta(1), finish = pd.to_datetime("12-30-2023"),Terminal_list = terminal_list)

    return edd_a_weekly_normalized,edd_a_weekly,edd_f_weekly_normalized,edd_f_weekly




def appending_files_in_folder(path_folder):
    arr = os.listdir(path_folder)
    df = pd.DataFrame()
    for i in arr:
        print('Appending .... {}'.format(i))
        df0 = pd.read_excel(os.path.join(path_folder,i))
        df0 = df0[1:]
        df = df.append(df0)
        
    return df



""" SQL functions ------------------------------------------------------------------------------------------------"""

def sql_reading_table(sql_query,mydb=mydb):
    """ Connect to your localhost and read the table using sql_query. 
    for example sql_query = 'Select * from fmr where .....  '"""
    df = pd.read_sql(sql_query,mydb)
    
    return df

def sql_read_data_from_mysql(table_name, database_url = database_url):
    """
    Read data from a MySQL database table and return it as a DataFrame.

    Parameters:
    - table_name: The name of the MySQL table you want to read data from.
    - database_url: The database URL including credentials (e.g., 'mysql+pymysql://username:password@localhost/database_name').

    Returns:
    - A pandas DataFrame containing the data from the specified table.
    """
    try:
        engine = create_engine(database_url)
        connection = engine.connect()
        query = text(f"SELECT * FROM {table_name};")
        result = connection.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        connection.close()
        return df
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None



def sql_appending_table(dataframe, table_name, if_exists='append',database_url=database_url):
    """
    Append a DataFrame to a MySQL database table.

    Parameters:
    - dataframe: The pandas DataFrame you want to append.
    - table_name: The name of the MySQL table you want to append to.
    - database_url: The database URL including credentials (e.g., 'mysql+pymysql://username:password@localhost/database_name').
    - if_exists: Action to take if the table already exists ('fail', 'replace', or 'append').

    Returns:
    - None
    """
    # Change these two sections to your local database
    # User: root
    # Pass : 123456
    try:
        engine = create_engine(database_url)
        dataframe.to_sql(name=table_name, con=engine, if_exists=if_exists, index=False)
        print(f"DataFrame successfully appended to {table_name} table in the database.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        
    #cols = "`,`".join([str(i) for i in df.columns.tolist()])
    #for i,row in df.iterrows():
    #    sql = "INSERT INTO `" + table_name + "` (`" + cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    #    #sql = "INSERT INTO `courierops` (`" + cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    #    cursor.execute('SET SQL_SAFE_UPDATES = 0;')
    #    cursor.execute(sql, tuple(row))
        # the connection is not autocommitted by default, so we must commit to save our changes
    #    connection.commit()
        



def sql_truncating_table(table_name,ask_user=1,cursor=cursor,connection=connection_type2):
    """example: table_name = 'courierops' |
          ask_user=1 is just to make sure about removing data"""
    # Confirmation : Asking user 
    import tkinter as tk
    from tkinter import simpledialog

    if ask_user ==1 :
        ROOT = tk.Tk()
        ROOT.withdraw()
        USER_INP = simpledialog.askstring(title="Confirmation",
                                  prompt="Do you want to TRUNCATE\Removing all the data?:")
    else:
        USER_INP ='y'
    if USER_INP.lower() in ['yes','y',1]:
        query1 = "DELETE FROM  `" + table_name + "` "
        cursor.execute('SET SQL_SAFE_UPDATES = 0;')
        cursor.execute(query1)
        connection.commit()
        print("Data was deleted :D")
        
    else:
        print('Go and eat somethig, You are tired :D')
    
def sql_removing_from_table(query, cursor= cursor,connection = connection_type2):
    """ query should be like this
    #query = "DELETE FROM `fmr_accuracy` where `fmr_accuracy`.`CalendarDate`>='%s'" %start """
    cursor.execute(query)
    connection.commit()
    

def sql_updating_table(table_name,df):
    """ Here we use Date column to remove records from sql table and append df"""
    df = df.sort_values(['Date'])
    starting_date = df.loc[0,'Date']
    query = "Delete From {} where Date >= {} ".format(table_name,starting_date)
    sql_removing_from_table(query, cursor= cursor,connection = connection_type2)
    sql_appending_to_table(df,table_name,cursor=cursor,connection=connection_type2)




def preprocessing_courierops(df):
    df = df.reset_index()
    if pd.isnull(df.loc[1,"CalendarDate"]) : df = df.iloc[1:,:] # remove the first line :total row
    df['Couriers/Day'] = df['Couriers/Day'].replace("-",0)
    df['Kilometers'] = df['Kilometers'].replace("-",0)
    terminal_list = sql_reading_table("select * from terminal_list")['Terminal'].sort_values()
    terminal_list = terminal_list.reset_index(drop=True)
    df = pd.merge(df,terminal_list,how='inner')
    df['Total Del Stops'] = df['PCL Del Stops'] + df['Agent Del Stops']
    df['Total PU Stops'] = df['PCL PU Stops'] + df['Agent PU Stops']
    df['Total Del Pcs'] = df['PCL Del Pcs'] + df['Agent Del Pcs']
    df['Total PU Pcs'] = df['PCL PU Pcs'] + df['Agent PU Pcs']
    df['TOTAL STOPS'] = df['Total Del Stops'] + df['Total PU Stops']

    df = df[['CalendarDate','Terminal','Total Del Stops','PCL Del Stops','Agent Del Stops', 
                                    'Total Del Pcs','PCL Del Pcs','Agent Del Pcs',
                                    'Total PU Stops','PCL PU Stops','Agent PU Stops',
                                    'Total PU Pcs' ,'PCL PU Pcs','Agent PU Pcs',
                                    'TOTAL STOPS',
                                    'Courier AM Hours', 'Courier Delivery Hours','Courier Pickup Hours',
                                    'Courier PM Hours', 'Courier Other Hours','Couriers/Day',
                                    'Courier count worked', 'WorkingDays', 'Kilometers',
                                    'AM Dock Hours', 'PM Dock Hours','Linehaul Hours', 
         ]]
    df = df.sort_values(['CalendarDate','Terminal'])
    df.set_index('CalendarDate',inplace=True)
    

    return df


def preprocessing_amazon(df,start,finish):
    df = df.iloc[2:,:] # remove the first line -total row
    df = changing_column_names(df)
    
    df = df.rename(columns={'Delivery Pieces':'Pieces','Delivery Stops':'Stops'})
    df = df[['Date','Terminal','Tier2','Pieces','Stops']]
    
        #df['Date'] = pd.to_datetime(df['Date'])
    df = df[~df['Terminal'].isin(['POR'])]
    df['Terminal'] = df['Terminal'].astype('int64')

    terminal_list = sql_reading_table("select * from terminal_list")['Terminal'].sort_values()
    terminal_list = terminal_list.reset_index(drop=True)
    df = pd.merge(df,terminal_list,how='inner')



    #terminal_list = sql_reading_table("select * from terminal_list")['Terminal'].sort_values()
    #terminal_list = terminal_list.reset_index(drop=True)
    
    #df = pd.merge(df,terminal_list,how='inner')

    # Slice dates based on the interval
    df = df[ (df['Date']>= start) & (df['Date']<=finish) ]

    df = df.sort_values(['Date','Terminal'])
    #df.set_index('Date',inplace=True)
    return df

def preprocessing_converting_ordinaldates(df):
    df = df.dropna()
    df['Date'] = df['Date'].astype('int64')
    df['Date_new'] = df['Date'].apply(lambda x : datetime.fromordinal (   datetime(1900, 1, 1).toordinal() + x - 2    ).strftime("%m-%d-%Y") )
    df = df.drop('Date',axis=1)
    df = df.rename(columns={'Date_new':'Date'})
    df['Date'] = pd.to_datetime(df['Date'])
    

    return df

def preprocessing_forecast(df,amazon):
    cols = df.columns
    cols_new = [i.replace("."," ") for i in cols]
    df.columns = cols_new
    df = df.rename(columns = {'Couriers per Day':'Couriers/Day'})

    df['Agent Del Stops'] = df['Total Del Stops'] - df['PCL Del Stops']
    df['Agent PU Stops'] = df['Total PU Stops'] - df['PCL PU Stops']
    df['TOTAL STOPS'] = df['Total Del Stops'] + df['Total PU Stops']

    df = df[['Date','Terminal','Total Del Stops','PCL Del Stops','Agent Del Stops', 
                                        'Total Del Pcs','PCL Del Pcs','Agent Del Pcs',
                                        'Total PU Stops','PCL PU Stops','Agent PU Stops',
                                        'Total PU Pcs' ,'PCL PU Pcs','Agent PU Pcs',
                                        'TOTAL STOPS',
                                        'Courier AM Hours', 'Courier Delivery Hours','Courier Pickup Hours',
                                        'Courier PM Hours', 'Courier Other Hours','Couriers/Day',
                                        'AM Dock Hours', 'PM Dock Hours' 
            ]]

    df['Date'] = pd.to_datetime(df['Date'])
    df = pd.merge(df,amazon,on=['Date','Terminal'],how='left')
    df.set_index('Date',inplace=True)
    df['Amazon Forecast Pieces'] = df['Amazon Forecast Pieces'].replace('-',0)
    df['Amazon Forecast Pieces'] = df['Amazon Forecast Pieces'].fillna(0)
    df['Amazon Forecast Pieces'] = df['Amazon Forecast Pieces'].astype('int64')
    df = df.mask(df < 0, 0)
    new_columns = df.columns
    new_columns = ["Forecast-"+i for i in df.columns]
    df.columns = new_columns
    df = df.rename(columns = {'Forecast-Terminal':'Terminal'})

    return df



def preprocessing_amazon_returns(df):
    df['Terminal'] = df['PODEventTerminal']
    df.loc[ (df['PODEventTerminal']==568 ) and (df['Terminal#']==507),'Terminal'] = 507
    df.loc[ (df['PODEventTerminal']==568 ) and (df['Terminal#']==541),'Terminal'] = 541
    df.loc[ (df['PODEventTerminal']==568 ) and (df['Terminal#']==511),'Terminal'] = 511

    df.loc[ (df['PODEventTerminal']==573 ) and (df['Terminal#']==507),'Terminal'] = 507
    df.loc[ (df['PODEventTerminal']==573 ) and (df['Terminal#']==541),'Terminal'] = 541
    df.loc[ (df['PODEventTerminal']==573 ) and (df['Terminal#']==511),'Terminal'] = 511

    df = df.rename(columns={'PODEventDate':'Date','Delivery Pieces':'AReturn Pieces'})
    df = df[['Date','Terminal','AReturn Pieces']]
    return df



def python_wday_to_purolator_wday(num):

    weekday_change = {6:1,0:2,1:3,2:4,3:5,4:6,5:7}
    return weekday_change[num]

def graph_plotting_daily_distribution(df,weeks,year):
    df = changing_column_names(df)
    if 'Week' not in df.columns : df1 = adding_year_week(df)
    df_new =df1[df1['Week'].isin(weeks)]
    df_new =df_new[df_new['Year'].isin([year])]
    df_new['Day_week'] = df_new['Date'].dt.weekday
    weekday_change = {6:1,0:2,1:3,2:4,3:5,4:6,5:7}
    df_new['Day'] = df_new['Day_week'].apply(lambda x :weekday_change[x] )
    df_new_weekly = df_new.groupby(['Week','Day'])['Pieces'].sum()
    
    df_new_weekly = df_new_weekly.reset_index()

    fig = px.line(df_new_weekly, x='Day', y='Pieces',
                        markers=True,
                        #line_dash ='Week',
                        color= 'Week',
                        color_discrete_sequence=px.colors.qualitative.G10,line_shape='linear',
                        title= "Daily distribution by weeks"
                        )
    fig.show()

    df_aggregate = df_new.groupby(['Day'])['Pieces'].sum()
    df_aggregate = df_aggregate.reset_index()

    fig1 = px.line(df_aggregate, x='Day', y='Pieces',
                        markers=True,
                        #line_dash ='Week',
                        #color= 'Week',
                        color_discrete_sequence=px.colors.qualitative.G10,line_shape='linear',
                        title = "Aggregated daily distribution")
    fig1.show()



def accuracy_weekly_data(actuals,forecast,comparing_weeks,year=2023):

    """ This is a function for weekly : dataframe must have these columns : ['Year', 'Week','Terminal','Customer,'Pieces']"""

    actuals = actuals[(actuals['Week'].isin(comparing_weeks)) & (actuals['Year']==year)]
    actuals = actuals.rename(columns= {'Pieces':'actual value'})
    forecast = forecast[(forecast['Week'].isin(comparing_weeks)) & (forecast['Year']==year)]
    forecast  = forecast.rename(columns = {'Pieces':'forecast value'})

    forecast = pd.merge(forecast,actuals,on=['Year','Week','Terminal','Customer'])[['Year','Week','Terminal','Customer','forecast value','actual value']]
    mea_score = np.round(mean_absolute_error(forecast['forecast value'],forecast['actual value']),2)
    mape_score = np.round(mean_absolute_percentage_error(forecast['forecast value'],forecast['actual value']),2)
    
    forecast['%error'] = np.round(np.absolute(forecast['actual value']/forecast['forecast value']-1),2)
    mean_error = np.round(np.average(forecast['%error']),2)
    forecast['error'] = forecast['actual value'] - forecast['forecast value']
    

    return mea_score,mape_score,mean_error,forecast

def analysis_holiday_effect(df,var,showing_chart=1):
    df ['Holiday'] = 0
    df.loc[ ~(df['Working Days'].isin([5,7]))  ,'Holiday']  = 1
    Holidays_effect = df.groupby(['Holiday'])[var].mean().reset_index()
    
    if showing_chart==1:
        fig_holiday_effct = px.bar(Holidays_effect,x='Holiday',y=[var],color='Holiday').show()
    
    return Holidays_effect

def adding_holiday(df):
    """Your table must have working days column"""
    df ['Holiday'] = 0
    df.loc[ ~(df['Working Days'].isin([5,7])),'Holiday']  = 1

    return df



def adding_cyberweek(df):
    cyberweek = { 2019:49,2020:49,2021:48,2022:48,2023:48}
    df['Cyber Week'] = 0
    for i in cyberweek :
        df.loc[    (df['Year']==i)    &   (df['Week']==cyberweek[i]) ,   'Cyber Week'] = 1
    return df



def adding_period(df):

    conds = [df['Week'] <= 4 ,
             (df['Week'] > 4) & (df['Week'] <= 8),
             (df['Week'] > 8) & (df['Week'] <= 13),
             (df['Week'] > 13) & (df['Week'] <= 17),
             (df['Week'] > 17) & (df['Week'] <= 21),
             (df['Week'] > 21) & (df['Week'] <= 26),
             (df['Week'] > 26) & (df['Week'] <= 30),
             (df['Week'] > 30) & (df['Week'] <= 34),
             (df['Week'] > 34) & (df['Week'] <= 39),
             (df['Week'] > 39) & (df['Week'] <= 43),
             (df['Week'] > 43) & (df['Week'] <= 47),
             (df['Week'] >= 47)]

    choices = [1,2,3,4,5,6,7,8,9,10,11,12]

    df['Period'] = np.select(conds,choices)

    return df
    





def accuracy_weekly_comparing_dfs(actuals,forecast,year,weeks,customer=None):
    """ Create a dataframe with actuals weeks and a column for forecast"""

    actuals = actuals[(actuals['Year']==year) and (actuals['Week'].isin(weeks))]
    forecast = forecast[(forecast['Year']==year) and (forecast['Week'].isin(weeks))]

    if customer is None:
        actuals = actuals[['Year','Week','Terminal','Pieces']]
        forecast = forecast[['Year','Week','Terminal','Pieces']].rename(columns={'Pieces':['F_'+forecast.name]})

        df = actuals.merge(forecast,on=['Year','Week','Terminal'])
    else:
        actuals = actuals[['Year','Week','Terminal','Customer','Pieces']]
        forecast = forecast[['Year','Week','Terminal','Customer','Pieces']].rename(columns={'Pieces':['F_'+forecast.name]})

        df = actuals.merge(forecast,on=['Year','Week','Customer','Terminal'])        

    return df