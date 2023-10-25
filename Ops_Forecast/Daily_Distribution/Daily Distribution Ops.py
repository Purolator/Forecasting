import pandas as pd
from datetime import timedelta,date
import sys
import itertools
import calendar
import numpy as np
import config
root_folder  = config.root_folder; database_url = config.database_url
sys.path.insert(0, root_folder)
import functions
import helper_function


#################################################################################################################
selection_weeks=[31,30,28,23,19]
regular_weeks = {
                 'ATLANTIC'             :{2023 :[35,34,33,31,30,29,28,26,25]},    # Also, you can use this format"'ATLANTIC' : {2023:[4,5],2021:[10,11,14]}""
                 'SOUTH WESTERN ONTARIO':{2023 :[35,33,31,30,28,25,24,22]},
                 'NORTH EASTERN ONTARIO':{2023 :[35,34,33,31,30,29,28,25]},
                 'GREATER TORONTO AREA' :{2023 :[34,33,31,30,28,25,24,22]},
                 'PACIFIC'              :{2023 :[35,33,31,29,28,25,24,23,]},
                 'PRAIRIES'             :{2023 :[35,34,33,31,30,29,28,26,25]},
                 'QUEBEC'               :{2023 :[35,34,33,31,30,28,25]},
                 }


no_agents_terminals = [12,30,40,50,51,52,53,120,121,136,147,171,193,194,207,210,219,
                       480,490,491,492,501,503,511,517,519,522,558,560]
one_stops_terminals=[216,217,521,523,]

events_weeks = helper_function.events_weeks
events_weeks_future = helper_function.events_weeks_future

#################################################################################################################
print("Start")

# fmr data 
[fmr,variables_list] = helper_function.pulling_fmr_data()
#fmr = fmr[fmr['Terminal'].isin([514])]

""" PART 1 : Creating Current Distribution """
fmr_regular_weeks           = helper_function.creating_dataset_for_regular_distribution(fmr,regular_weeks)
dis_regular_weeks           = helper_function.creating_regular_distribution(fmr_regular_weeks)


""" PART 2 : Creating Events Distribution """
fmr_events_weeks            = helper_function.creating_dataset_for_events_distribution(fmr,events_weeks)
dis_events_weeks            = helper_function.creating_events_distribution_table(fmr_events_weeks,events_weeks)


""" PART 3 : Creating year_week Table """
year_week_structure_tbl     = helper_function.year_week_structure(2023,2024,events_weeks_future)
year_week_structure_tbl.to_parquet('year_week_structure_tbl.parquet')
year_week_structure_tbl = pd.read_parquet('year_week_structure_tbl.parquet')
year_week_events_tbl        = helper_function.year_week_events_tbl(year_week_structure_tbl,events_weeks_future)


""" PART 4: Appending Current and Event Distribution """
distribtion_events_table    =  year_week_events_tbl.merge(dis_events_weeks,on=['Year','Terminal','Wday','event_name'],how='left')
regular_week_tbl            =  year_week_structure_tbl.set_index(['Year','Week','Province']).drop(year_week_events_tbl.set_index(['Year','Week','Province']).index).reset_index()
distribtion_regular_table   =  pd.merge(regular_week_tbl,dis_regular_weeks,on=["Terminal","Wday"],how='left')
#distribtion_regular_table['event_name'] = "-"
distribution_tbl = pd.concat(
                            [
                            distribtion_regular_table[['Year','Week','Terminal','Wday']+variables_list+['event_name','Province']],
                            distribtion_events_table[['Year','Week','Terminal','Wday']+variables_list+['event_name','Province']]
                            ]
                            )


#Exceptional cases : in case you need to replace division|terminal distribution with a better one
distribution_tbl = helper_function.replacing_old_dis_with_new_dist(fmr,distribution_tbl,target_year = 2023,target_week = 48,
                                    event_name="peak",division = "PACIFIC",terminal="ALL",dis_weeks={2021:[48]})

""" PART 5 : Special Editing """
termianls_without_weekend_op = [40,42,191,170]
termianls_without_weekend_op_sat = []
termianls_without_weekend_op_sun =[41,194,504,519,]



""" PART 6 : Daily Forecast """
# applying ditribution to denormalized data.
denormalized_df = functions.sql_reading_table("select * from forecast_results_denormalized")
helper_function.checking_negative_values(denormalized_df)
daily_forecast_tbl = helper_function.create_daily_forecast_tbl(denormalized_df,year_week_structure_tbl,distribution_tbl,termianls_without_weekend_op,termianls_without_weekend_op_sat,termianls_without_weekend_op_sun)

# Charts
helper_function.graph_plotting_daily_distribution(fmr,year_week={2021:[48],2022:[48],2020:[48],2019:[49]},division = 'PACIFIC',terminal= "ALL",variable="Total Del Stops")

# Fixing PCL DEL STOPS when its bigger than Total DEL Stops
daily_forecast_tbl = helper_function.editing_pcl_total_del_stops(daily_forecast_tbl,no_agents_terminals,one_stops_terminals)


# Checking the gap between forecast values and average of mean_weeks
gap_foercast_with_averages = helper_function.candidates_for_checking(daily_forecast_tbl,fmr,weeks=[42,43],year=[2023],mean_weeks=[34,35],mean_year=[2023],thr=0.2,err=500)


daily_forecast_tbl.to_excel('daily_forecast_tbl.xlsx')


""" PART 7 : Appending data to daily forecast table """
functions.sql_truncating_table("daily_forecast",ask_user=0,cursor=functions.cursor,connection=functions.connection_type2)
print('Starting appending data to daily forecast table')
functions.sql_appending_table(daily_forecast_tbl, "daily_forecast", if_exists='replace',database_url=database_url)
print('Daily forecast table is updated')


