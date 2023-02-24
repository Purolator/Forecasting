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
#import plotly.graph_objects as go
#from plotly.subplots import make_subplots
import plotly.io as io
io.renderers.default='browser' # or you can use 'svr'
import plotly.express as px
#import pyarrow.parquet as pq
from pmdarima import auto_arima # for determining ARIMA orders
from statsmodels.tsa.api import STLForecast,ARIMA
import mysql.connector as connection_type2
import pymysql

##########################################################################################################
"""  update this section to your computer  """
dir_folder = "C:/Forecast tools/Python Projects/New Model/"
"""
You need two files:
    'DailyHolidayData.parquet''
    'HolidayData.parquet'
    ###'TerminalProvinces.xlsx' Special cases 
"""
# This section is for Ops forecast
connection = pymysql.connect(host='localhost',
                             user='root',        # update this one
                             password='123456',  # update this one
                             db='forecasting_schema')
cursor = connection.cursor()

mydb = connection_type2.connect(host="localhost", database = 'forecasting_schema',user="root", passwd="123456",use_pure=True)

##########################################################################################################

def preprocessing_data(df,start,finish,customer=False,terminal=False):
    df = changing_column_names(df)
    print('Changing column names .................. done!')
    df = adding_year_week(df)
    print('adding year and week to data .................. done!')

    df = adding_Workingdays_province(df)
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
        if i.lower() in ['calendardate','date','calendar date','induction date','edd','eid','expected delivery date']:
            df = df.rename(columns={i:'Date'})
            df['Date'] = pd.to_datetime(df['Date'])
            #df.set_index('Date',inplace=True)
        
        if i.lower() in ['week number','week','weeknumber']:
            df = df.rename(columns={i:'Week'})
            df['Week'] = df['Week'].astype(int)
            
        if i.lower() in ['year'] :
            df = df.rename(columns={i:'Year'})
            df['Year'] = df['Year'].astype(int)
        
        if i.lower() in ['customer','master client']:
            df = df.rename(columns={i:'Customer'})
            
        
        if i.lower() in ['termnr','origindepotid',"terminal",'destdepotid','Terminal']:
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


def adding_Workingdays_province(df):
        
    Terminal_list = pd.read_excel(os.path.join(dir_folder, "TerminalProvinces.xlsx"))
    Terminal_list = changing_column_names(Terminal_list)
    df = df.merge(Terminal_list)

    HolidayData=pd.read_parquet(os.path.join(dir_folder,'HolidayData.parquet'))
    HolidayData = changing_column_names(HolidayData)
    df = df.merge(HolidayData[['Year','Week','Province','Working Days']], how='left',on = ['Year','Week','Province'])

    return df



def defining_period(df,start,finish):
    ''' Defining Start and End of historical data '''
    df = df.loc[( df['Date']>= datetime.strptime(start,'%m-%d-%Y') ) & ( df['Date']<= datetime.strptime(finish,'%m-%d-%Y')  )]
    
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
    df_weekly_normalized = df_weekly_normalized[['Date','Year-Week','Customer','Terminal','Normalized Pieces']]
    df_weekly_normalized = df_weekly_normalized.rename(columns={'Normalized Pieces':'Pieces'})
    
    
    
    df_weekly_normalized = df_weekly_normalized[['Date','Year-Week','Customer','Terminal','Pieces']].set_index('Date')
    df_weekly.set_index('Date',inplace=True)
    return df_weekly_normalized,df_weekly


def filling_missing_values(df,start,finish):
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
    date = datetime.strptime(date,'%m-%d-%Y')
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
                        title=" Terminal {} - Customer {} ".format(df.Terminal.unique(),df.Customer.unique()))
    fig.show()
    


def graph_checkings(df):
    df.loc[(df['Pieces']<=-0.00001) & (df['Pieces']>=-7),'Pieces'] = 0
    
    return df



def denormalize_data(df):
    df = adding_Workingdays_province(df)
    df['Denormalized Pieces'] = np.round((df['Pieces'] /5) * df['Working Days'])
    df = df[['Year','Week','Customer','Terminal','Denormalized Pieces','Type']].rename(columns={'Denormalized Pieces':'Pieces'})
    return df




def appending_actuals_forecast(actual,forecast,customer = None,terminal=None, finish=None):
    actual ['Type'] = 'Actual'
    
    end_year=  returning_weeknumber(finish)[0]
    end_week = returning_weeknumber(finish)[1]
    forecast = pd.DataFrame(forecast).reset_index()
    forecast.columns = ['Date','Pieces']
    forecast.set_index('Date',inplace=True)
    forecast ['Type'] = 'Forecast'
    
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


def stepwise_forecast_model(df,horizon):
    stepwise_model = auto_arima(df, start_p=1, start_q=1,
                           max_p=3, max_q=3, m=52,
                           start_P=0, seasonal=True,
                           d=1, D=1, trace=True,
                           error_action='ignore',  
                           suppress_warnings=True, 
                           stepwise=True)
    #print(stepwise_model.aic())

    forecasts = stepwise_model.predict(horizon)
    
    return forecasts



def combination_stepwise_STLF(df,horizon):
    stepwise_fit = auto_arima(df, start_p=0, start_q=0,
                                        max_p=6, max_q=3, m=52,
                                        seasonal=False,
                                        d=None, trace=False,
                                        error_action='ignore',   # we don't want to know if an order does not work
                                        suppress_warnings=True,  # we don't want convergence warnings
                                        stepwise=True)           # set to stepwise
    
    stlf = STLForecast(df, ARIMA,model_kwargs={"order": (stepwise_fit.arima_res_.model_orders['ar']
                                                             , stepwise_fit.arima_res_.model_orders['ma']
                                                             ,stepwise_fit.arima_res_.model_orders['variance'] )},seasonal=5)




    
    stlf_res = stlf.fit()
    forecasts = stlf_res.forecast(horizon)
    
    return forecasts
    
    
def amazon_regressor(start,finish,terminal=None):   
    
    
    start_current_year="2023-01-01"
    
    amz_act = pd.read_parquet('Amazon Actuals Daily.parquet')
    amz_fc = pd.read_parquet('Amazon Forecast Daily.parquet')
    
    
    
    amazon_current_year = pd.read_excel(r'C:\Users\mehrdad.dadgar\Downloads\P&D and Amazon dashboard New Version (3).xlsx',sheet_name='Data',usecols=['Date','Terminal Number','Amazon Forecast Pieces','Scanned Data ( Tier 1 + Tier 2)'])
    amazon_current_year = amazon_current_year[['Date','Terminal Number','Amazon Forecast Pieces','Scanned Data ( Tier 1 + Tier 2)']]
    amazon_current_year = amazon_current_year.rename(columns ={'Terminal Number':'Terminal','Amazon Forecast Pieces':'Forecast','Scanned Data ( Tier 1 + Tier 2)':'Actual'})
    #amazon_current_year_act = amazon_current_year[amazon_current_year['Date']>=start_current_year][['Date','Terminal','Actual']].rename(columns = {'Actual':'Sum of Pieces'})
    amazon_current_year_fc = amazon_current_year[amazon_current_year['Date']>=start_current_year][['Date','Terminal','Forecast']].rename(columns = {'Forecast':'Sum of Pieces'})
    
    
    amz_act_new = amz_act[amz_act["Date"]<="2020-12-31"]
    amz_fc_new = amz_fc[amz_fc["Date"]>"2020-12-31"]
    
    amazon_regressor  = pd.concat([amz_act_new,amz_fc_new,amazon_current_year_fc])
    amazon_regressor = amazon_regressor[amazon_regressor['Date']>=start]
    amazon_regressor = amazon_regressor[amazon_regressor['Date']<=finish]

    if terminal != None : amazon_regressor = amazon_regressor[amazon_regressor['Terminal']==terminal]
    amazon_regressor = amazon_regressor.rename(columns= {'Date':'Calendar Date'})
    
    return amazon_regressor


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


def sql_appending_to_table(df,table_name ='courierops' ,cursor=cursor,connection=connection):
    """ dataframe must have the same columns  and table_name is a string for example: table_name='fmr' """
    cols = "`,`".join([str(i) for i in df.columns.tolist()])
    for i,row in df.iterrows():
        sql = "INSERT INTO `" + table_name + "` (`" + cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
        #sql = "INSERT INTO `courierops` (`" + cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
        cursor.execute('SET SQL_SAFE_UPDATES = 0;')
        cursor.execute(sql, tuple(row))
        # the connection is not autocommitted by default, so we must commit to save our changes
        connection.commit()


def sql_truncating_table(table_name,ask_user=1,cursor=cursor,connection=connection):
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
    if USER_INP.lower() in ['yes','y',1]:
        query1 = "DELETE FROM  `" + table_name + "` "
        cursor.execute(query1)
        print("Data was deleted :D")
        connection.commit()
    else:
        print('Go and eat somethig, You are tired :D')
    
def sql_removing_from_table(query, cursor= cursor,connection = connection):
    """ query should be like this
    #query = "DELETE FROM `fmr_accuracy` where `fmr_accuracy`.`CalendarDate`>='%s'" %start """
    cursor.execute(query)
    connection.commit()