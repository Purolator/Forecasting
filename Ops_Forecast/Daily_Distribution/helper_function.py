import pandas as pd
import numpy as np
import sys
import os
import itertools
import config
root_folder  = config.root_folder
sys.path.insert(0, root_folder)
import functions  
import plotly.io as io
io.renderers.default='browser' # or you can use 'svr'
import plotly.express as px
import plotly.graph_objects as go


#################################################################################################################
""" USER INPUTS"""
ALL                     = ['AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'NT', 'ON', 'PE', 'QC', 'SK', 'YT']
family_day_provinces    = ['AB','SK','MB','ON','BC']
civic_provinces         = ['AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'NT', 'ON', 'PE','SK', 'YT']
rememberance_provinces  = ['BC']
events_weeks = {
                    2023:
                    {
                        'New Years Day' :                   {2022:[1]},
                        'family_day'  :                     {2022:[8]}, # You can use this pattern as well {2022:[8],2021:[7]} to take the average of two weeks 
                        'good_friday' :                     {2022:[15]},
                        'victoria_day':                     {2022:[21]},
                        'St Jean Baptiste Day':             {2022:[25]},
                        'canada_day'  :                     {2022:[26]},
                        'civic_day'   :                     {2022:[31]},
                        'labour_day'  :                     {2022:[36]},
                        'Truth_and_Reconciliation' :        {2022:[39]},
                        'Week_after_T&R':                   {2023:[15],2022:[40]},
                        'thanksgiving':                     {2022:[41]},
                        'rememberance_day':                 {2022:[45]},
                        'peak'        :                     {2023:[48],2021:[48]},
                        'christmas'    :                    {2022:[52]}
                    },
                     2024:
                    {
                        'New Years Day' :                   {2023:[1]},
                        'family_day'  :                     {2023:[8]},
                        'good_friday' :                     {2023:[14]},
                        'victoria_day':                     {2023:[21]},
                        'St Jean Baptiste Day':             {2023:[26]},
                        'canada_day'  :                     {2023:[27]},
                        'civic_day'   :                     {2023:[32]},
                        'labour_day'  :                     {2023:[36]},
                        'Truth_and_Reconciliation' :        {2022:[39]},
                        'Week_after_T&R':                   {2023:[15],2022:[40]},
                        'thanksgiving':                     {2022:[41]},
                        'rememberance_day':                 {2022:[45]},
                        'peak'        :                     {2022:[48],2021:[48]},
                        'christmas'    :                    {2022:[52]}
                 }
                }

events_weeks_future = {
                        2023:
                            {
                                'New Years Day':{               "Week":1   ,"Province":ALL},
                                'family_day':{                  "Week":8    ,"Province":family_day_provinces},
                                'good_friday':{                 "Week":14   ,"Province":ALL},
                                'victoria_day':{                "Week":21   ,"Province":ALL},
                                'St Jean Baptiste Day':{        "Week":26   ,"Province":['QC']},
                                'canada_day':{                  "Week":27   ,"Province":ALL},
                                'civic_day':{                   "Week":32   ,"Province":civic_provinces},
                                'labour_day':{                  "Week":36   ,"Province":ALL},
                                'Truth_and_Reconciliation':{    "Week":39   ,"Province":ALL},
                                'Week_after_T&R':{              "Week":40   ,"Province":ALL},
                                'thanksgiving':{                "Week":41   ,"Province":ALL},
                                'rememberance_day':{            "Week":45   ,"Province":rememberance_provinces},
                                'peak':{                        "Week":48   ,"Province":ALL},
                                'christmas':{                   "Week":52   ,"Province":ALL},

                                },
                        2024:
                            {
                                'New Years Day':{               "Week":1   ,"Province":ALL},
                                'family_day':{                  "Week":8    ,"Province":family_day_provinces},
                                'good_friday':{                 "Week":15   ,"Province":ALL},
                                'victoria_day':{                "Week":21   ,"Province":ALL},
                                'St Jean Baptiste Day':{        "Week":26   ,"Province":['QC']},
                                'canada_day':{                  "Week":21   ,"Province":ALL},
                                'civic_day':{                   "Week":31   ,"Province":civic_provinces},
                                'labour_day':{                  "Week":36   ,"Province":ALL},
                                'Truth_and_Reconciliation':{    "Week":39   ,"Province":ALL},
                                'Week_after_T&R':{              "Week":40   ,"Province":ALL},
                                'thanksgiving':{                "Week":41   ,"Province":ALL},
                                'rememberance_day':{            "Week":45   ,"Province":rememberance_provinces},
                                'peak':{                        "Week":48   ,"Province":ALL},
                                'christmas':{                   "Week":52   ,"Province":ALL},
                                },           
                        }

#################################################################################################################


terminal_division_list = functions.sql_reading_table("select * from terminal_list")
denormalized_df = functions.sql_reading_table("select * from forecast_results_denormalized limit 1")
variables_list = denormalized_df.columns.drop(['Year', 'Week', 'Terminal']).to_list()


def pulling_fmr_data(variable_list = variables_list):

    fmr = functions.sql_reading_table("select * from fmr where CalendarDate>='2021-01-01'; ")
    fmr = fmr[["CalendarDate","Terminal"]+variables_list]
    dates = pd.date_range('2021-01-01',max(list(fmr['CalendarDate'])))

    date_teminal_list = pd.DataFrame(
                        itertools.product(dates,terminal_division_list['Terminal'])
                        ).reset_index(drop=True).rename(columns={0:'CalendarDate',1:'Terminal'})


    fmr = pd.merge(date_teminal_list,fmr,on=['CalendarDate','Terminal'],how='left').fillna(0)
    fmr = pd.merge(fmr,terminal_division_list[['Terminal','Division','Province']],how='left',on='Terminal')

    fmr['Wday'] = fmr["CalendarDate"].dt.weekday
    fmr['Wday'] =fmr['Wday'].map(functions.python_wday_to_purolator_wday)
    fmr = functions.adding_year_week(fmr.rename(columns = {'CalendarDate':'Date'}))
    
    return fmr,variables_list




def creating_dataset_for_regular_distribution(fmr,regular_weeks):
    """ Create a new data frame having regular weeks

    Args:
        fmr (dataframe): fmr table
        regular_weeks (_type_): The past few weeks that 
        we will use them to calculate regular weeks distribution

    Returns:
        fmr_regular_weeks: filtering regular weeks on fmr table
    """
    fmr_regular_weeks = pd.DataFrame()
    for div in regular_weeks.keys():
        temp = fmr[fmr["Division"].isin(list([div]))]
        for yr in regular_weeks[div].keys():
            temp_yr = temp[temp["Year"]==yr]
            temp_yr = temp_yr[temp_yr["Week"].isin(regular_weeks[div][yr])]
            fmr_regular_weeks = pd.concat([fmr_regular_weeks,temp_yr])
    
    return fmr_regular_weeks



def creating_regular_distribution(fmr_regular_weeks,variables_list = variables_list):
    """Creating daily distribution for regular weeks

    Args:
        fmr_regular_weeks (DataFrame): filtering regular weeks on fmr table
        variables_list (list, optional): List of 8 variables. Defaults to variables_list.

    Returns:
        Dataframe: daily distribution for regular weeks
    """
    distribution = pd.DataFrame(fmr_regular_weeks,variables_list)
    first_run=1
    for var in variables_list:
        fmr_daily = fmr_regular_weeks[['Year','Terminal','Week','Wday']+[var]]

        # first aggregate for each year to wday level
        fmr_daily = fmr_daily.groupby(['Year','Terminal','Wday'])[var].sum().reset_index()

        #Get rid of weekdays and aggregate to the yearly level
        fmr_regular_weeks_agg = fmr_daily.groupby(['Year','Terminal'])[var].sum().reset_index()
        fmr_daily = pd.merge(fmr_daily,fmr_regular_weeks_agg,on=['Year','Terminal'],how='left')
        fmr_daily[var] = fmr_daily[var+"_x"]/fmr_daily[var+"_y"]

        fmr_dis = fmr_daily.groupby(['Terminal',"Wday"])[[var]].mean().reset_index()
        if first_run == 1:
            distribution = fmr_dis
            first_run =0
        else:
            distribution = pd.merge(distribution,fmr_dis,on=['Terminal','Wday'],how='left')
        
    distribution = distribution.fillna(0)
    return distribution

    
# Create events weeks distribution
def creating_dataset_for_events_distribution(fmr,events_weeks):
    """Create a new data frame having historical weeks based on `events_weeks` variable

    Args:
        fmr (Dataframe):fmr table
        events_weeks (Dictionary): weeks we would like to use to calculate daily distribution

    Returns:
        DataFrame: filtering weeks having events on fmr table
    """
    fmr_events_weeks = pd.DataFrame()
    for yr in events_weeks.keys():
        for event in events_weeks[yr].keys():
            for yr_historical in events_weeks[yr][event].keys():
            
                temp = fmr[(fmr['Year']==yr_historical) & (fmr['Week'].isin(events_weeks[yr][event][yr_historical]))]
                temp['Year_distribution'] = yr
                temp['event_name'] = event
                fmr_events_weeks = pd.concat([fmr_events_weeks,temp])
    
    return fmr_events_weeks



def creating_events_distribution_table(fmr_events_weeks,events_weeks):
    """reating daily distribution for events weeks

    Args:
        fmr_events_weeks (Dataframe): filtering weeks having events on fmr table
        events_weeks (Dictionary): weeks we would like to use to calculate daily distribution

    Returns: daily distribution for events
    """
    events_distribution = pd.DataFrame()
    for yr in events_weeks.keys():
        for event in events_weeks[yr].keys():
            temp = fmr_events_weeks[ (fmr_events_weeks['Year_distribution']==yr) & 
                                    (fmr_events_weeks['event_name']==event)]
            temp_dis = creating_regular_distribution(temp)
            temp_dis['Year'] = yr
            temp_dis['event_name']= event
            events_distribution = pd.concat([events_distribution,temp_dis])

    return events_distribution


def year_week_structure(start_year,end_year,events_weeks_future):

    years_list = [start_year,end_year]
    weeks_list = np.arange(1,53)
    terminal_list = functions.sql_reading_table("select * from terminal_list")
    wday_list = np.arange(1,8)
    year_week_structure = pd.DataFrame(itertools.product(terminal_list['Terminal'].unique(),years_list,weeks_list,wday_list)).reset_index(drop=True)
    year_week_structure.columns = ['Terminal','Year','Week','Wday']
    year_week_structure = pd.merge(year_week_structure,terminal_list[['Terminal','Province']],on=['Terminal'],how='left')
    
    year_week_structure['event_name'] = "-"
    for yr in events_weeks_future.keys():
        for event in events_weeks_future[yr].keys():
            week_nr = events_weeks_future[yr][event]['Week']
            pr = events_weeks_future[yr][event]['Province']
            year_week_structure.loc[  (year_week_structure['Year']==yr) & (year_week_structure['Week']==week_nr) & (year_week_structure['Province'].isin(pr)),'event_name'] = event


    #For test
    #year_week_structure = year_week_structure[year_week_structure['Terminal']==514]
    return year_week_structure

def year_week_events_tbl(year_week_structure_tbl,events_weeks_future):
    year_week_events_tbl = pd.DataFrame()
    for yr in events_weeks_future.keys():
        for hol in events_weeks_future[yr].keys():
            week = events_weeks_future[yr][hol]['Week']
            prov = events_weeks_future[yr][hol]['Province']

            temp = pd.DataFrame(itertools.product([yr],[hol],[week],prov)).reset_index(drop=True)
            year_week_events_tbl = pd.concat([year_week_events_tbl,temp])
    year_week_events_tbl.columns = ['Year','event_name','Week','Province']      
    year_week_events_tbl =pd.merge(year_week_events_tbl,year_week_structure_tbl,on=['Year','Week','Province','event_name'],how='left')
    
    #For Test
    #year_week_events_tbl = year_week_events_tbl[year_week_events_tbl['Province']=="AB"]
     
    return year_week_events_tbl




def create_daily_forecast_tbl(denormalized_df,year_week_structure_tbl,distribution_tbl,termianls_without_weekend_op,termianls_without_weekend_op_sat,termianls_without_weekend_op_sun):
    """ This function will covnert weekly numbers to daily.

    Args:
        denormalized_df (DataFrame): Denormalized weekly volumes
        year_week_structure_tbl (_type_): _description_
        distribution_tbl (DataFrame): daily distribution table
        termianls_without_weekend_op (_type_): List of termianls we would like to remove weekend operations volume

    Returns:
        DataFrame: final daily forecast table, ready to store in SQL
    """
    daily_forecast_tbl=pd.DataFrame()
    first_run = 1 
    for col in variables_list:
        selected_columns = ['Terminal','Year','Week']+[col]
        temp = denormalized_df[selected_columns]
        daily_table = pd.merge(year_week_structure_tbl,temp,on=['Terminal','Year','Week'])
        daily_table =pd.merge(daily_table,distribution_tbl[selected_columns+["Wday"]],on=['Terminal','Year','Week','Wday'])
        daily_table[col] = round(daily_table[col+"_x"]*daily_table[col+"_y"],1)
        daily_table = daily_table.drop([col+"_x"]+[col+"_y"],axis=1)
        if first_run == 1:
            daily_forecast_tbl = daily_table
            first_run =0
        else:
            daily_forecast_tbl = pd.merge(daily_forecast_tbl,daily_table[['Terminal','Year','Week','Wday']+[col]],
                                        on=['Terminal','Year','Week','Wday'],how='left')
        
    daily_forecast_tbl = zero_out_weekends(daily_forecast_tbl,termianls_without_weekend_op,type='both')
    daily_forecast_tbl = zero_out_weekends(daily_forecast_tbl,termianls_without_weekend_op_sat,type='Sat')
    daily_forecast_tbl = zero_out_weekends(daily_forecast_tbl,termianls_without_weekend_op_sun,type='Sun')

    year_week_wday = pd.read_parquet('year_week_wday.parquet')
    daily_forecast_tbl = daily_forecast_tbl.merge(year_week_wday,on=['Year',"Week","Wday"],how="left")
    daily_forecast_tbl = daily_forecast_tbl.merge(terminal_division_list[['Terminal','Division','District','Terminal Name']],how="left")
    daily_forecast_tbl = daily_forecast_tbl[['Calendar Date','Terminal','Division','District','Terminal Name']+variables_list]
    daily_forecast_tbl = daily_forecast_tbl.sort_values(by=["Calendar Date","Terminal"])
    return daily_forecast_tbl



# Special Adjustments
def zero_out_weekends(daily_forecast_tbl,termianls_without_weekend_op,type="both"):
    """This function will zero out weekends volumes for provided list of terminals; Feel free to create another function to shift these volumes to other working days.

    Args:
        daily_forecast_tbl (dataframe): Daily Forecast table
        termianls_without_weekend_op (List): List of termianls we would like to remove weekend operations

    Returns:
        _type_: Edited Daily Forecast table without weekend operations for defined terminal
    """
    if type=="both":
        daily_forecast_tbl.loc[  ( daily_forecast_tbl['Wday'].isin([1,7])) &
                                ( daily_forecast_tbl['Terminal'].isin(termianls_without_weekend_op) )&
                                (daily_forecast_tbl['event_name']=="-") ,variables_list] =0
    
    if type=="Sat":
        daily_forecast_tbl.loc[  ( daily_forecast_tbl['Wday'].isin([7])) &
                                ( daily_forecast_tbl['Terminal'].isin(termianls_without_weekend_op) )&
                                (daily_forecast_tbl['event_name']=="-") ,variables_list] =0

    if type=="Sun":
        daily_forecast_tbl.loc[  ( daily_forecast_tbl['Wday'].isin([1])) &
                                ( daily_forecast_tbl['Terminal'].isin(termianls_without_weekend_op) )&
                                (daily_forecast_tbl['event_name']=="-") ,variables_list] =0
    
    return daily_forecast_tbl



def editing_pcl_total_del_stops(daily_forecast_tbl,no_agents_terminals,one_stops_terminals,variables_list=variables_list):
    daily_forecast_tbl[variables_list] = daily_forecast_tbl[variables_list].astype(int)
    
    #if we have no agent del pcs, PCL should be equal to Total
    condition = (daily_forecast_tbl['PCL Del Stops']>daily_forecast_tbl['Total Del Stops']) & (daily_forecast_tbl['Agent Del Pcs']==0)
    daily_forecast_tbl.loc[condition,'PCL Del Stops'] = daily_forecast_tbl.loc[condition,'Total Del Stops']

    # For no_agent termianls zero out Agent Del Pcs 
    daily_forecast_tbl.loc[daily_forecast_tbl['Terminal'].isin(no_agents_terminals),'Agent Del Pcs'] = 0
    daily_forecast_tbl.loc[daily_forecast_tbl['Terminal'].isin(no_agents_terminals),'PCL Del Stops'] = daily_forecast_tbl.loc[daily_forecast_tbl['Terminal'].isin(no_agents_terminals),'Total Del Stops']

    # One Stops
    condition_pcl_grt_ttl = (daily_forecast_tbl['PCL Del Stops']>daily_forecast_tbl['Total Del Stops'])
    one_stop_condition = (daily_forecast_tbl['Terminal'].isin(one_stops_terminals))&(~daily_forecast_tbl['Calendar Date'].dt.weekday.isin([6,5]))
    condition_2 = condition_pcl_grt_ttl & one_stop_condition
    daily_forecast_tbl.loc[condition_2,'PCL Del Stops'] = daily_forecast_tbl.loc[condition_2,'Total Del Stops'] - 1 
    
    # This needs to be adjusted based on the recent trend !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    daily_forecast_tbl.loc[condition_pcl_grt_ttl,"PCL Del Stops"] = daily_forecast_tbl.loc[condition_pcl_grt_ttl,"Total Del Stops"] - daily_forecast_tbl.loc[condition_pcl_grt_ttl,"Agent Del Pcs"]/1.5

    return daily_forecast_tbl


def checking_negative_values(df):
    import warnings

    negative_columns = df.columns[df.lt(0).any()]
    if not negative_columns.empty: 
        #raise ValueError(f"Negative values found in columns: {', '.join(negative_columns)}")
        warning_message = f"Warning: Negative values found in columns: {', '.join(negative_columns)}"
        warnings.warn(warning_message, Warning)


def graph_plotting_daily_distribution(df,year_week={2021:[5,4],2022:[7,9,2,1]},division = 'ALL',terminal= "ALL",variable="Total Del Stops"):
    



    df = functions.changing_column_names(df)

    # Adding week 
    if 'Week' not in df.columns : df1 = functions.adding_year_week(df)
    
    # Filter on Division and Terminal
    if division!="ALL" :  df = df[df['Division']==division]
    if terminal !="ALL" : df = df[df['Terminal']==terminal]
    
    # Create an empty DataFrame to store the filtered results
    filtered_df = pd.DataFrame(columns=df.columns)

    # Iterate through the dictionary and filter the DataFrame
    for year, weeks in year_week.items():
        year_mask = df['Year'] == year
        week_mask = df['Week'].isin(weeks)
        filtered_df = filtered_df.append(df[year_mask & week_mask])

    #Adding wday
    filtered_df['Day_week'] = filtered_df['Date'].dt.weekday
    weekday_change = {6:1,0:2,1:3,2:4,3:5,4:6,5:7}
    filtered_df['Day'] = filtered_df['Day_week'].apply(lambda x :weekday_change[x] )


    # Create aggregated to the weekly
    filtered_df_weekly = filtered_df.groupby(['Year','Week','Day'])[variable].mean()
    filtered_df_weekly = filtered_df_weekly.reset_index()


    # Create the first line chart
    fig = px.line(filtered_df_weekly, x='Day', y=variable,
                markers=True,
                line_dash='Year',
                color='Week',
                color_discrete_sequence=px.colors.qualitative.G10, line_shape='linear',
                title="Daily distribution by weeks"
                )

    # Create the aggregated line chart
    df_aggregate = filtered_df_weekly.groupby(['Day'])[variable].mean().reset_index()
    fig1 = px.line(df_aggregate, x='Day', y=variable,
                markers=True,
                color_discrete_sequence=['blue'], line_shape='linear',
                title="Aggregated daily distribution")

    # Add the aggregated line to the first chart
    fig.add_trace(go.Scatter(x=fig1.data[0].x, y=fig1.data[0].y,
                            mode='lines',
                            name='Aggregated Line',
                            line=dict(color='green', dash='dot')))

    # Show the combined chart
    fig.show()
"""
    fig = px.line(filtered_df_weekly, x='Day', y=variable,
                        markers=True,
                        line_dash ='Year',
                        color= 'Week',
                        color_discrete_sequence=px.colors.qualitative.G10,line_shape='linear',
                        title= "Daily distribution by weeks"
                        )
    #fig.show()

    df_aggregate = filtered_df_weekly.groupby(['Day'])[variable].sum()
    df_aggregate = df_aggregate.reset_index()

    fig1 = px.line(df_aggregate, x='Day', y=variable,
                        markers=True,
                        #line_dash ='Week',
                        #color= 'Week',
                        color_discrete_sequence=px.colors.qualitative.G10,line_shape='linear',
                        title = "Aggregated daily distribution")
    fig1.show()
"""


def candidates_for_checking(daily_forecast_tbl,df,weeks=[42,43,45],year=[2023],mean_weeks=[34,35],mean_year=[2023],thr=0.3,err=500):
    """_summary_

    Args:
        daily_forecast_tbl (DataFrame): Daily forecast result
        df (DF): Historical data
        weeks (list, optional): Forecasting weeks to show the gap for them. Defaults to [42,43,45].
        year (list, optional): orecasting year to show the gap for it. Defaults to [2023].
        mean_weeks (list, optional): list of actuals weeks to calculate average of volumes for each day. Defaults to [34,35].
        mean_year (list, optional): _description_. Defaults to [2023].
        thr (float, optional): % error threshold. Defaults to 0.3.
        err (int, optional): error threshold. Defaults to 500.

    Returns:
        _type_: _description_
    """
    df = functions.changing_column_names(df)
    

    mean_two_weeks = df[(df['Week'].isin(mean_weeks)) & (df['Year'].isin(mean_year))]
    mean_two_weeks = mean_two_weeks.groupby(['Wday','Terminal'])["Total Del Stops"].mean().reset_index()
    mean_two_weeks = mean_two_weeks[['Wday','Terminal','Total Del Stops']]
    
    daily_forecast_tbl_comparison = functions.adding_year_week(daily_forecast_tbl.rename(columns={'Calendar Date':'Date'}))
    
    #Adding wday
    daily_forecast_tbl_comparison['Day_week'] = daily_forecast_tbl_comparison['Date'].dt.weekday
    weekday_change = {6:1,0:2,1:3,2:4,3:5,4:6,5:7}
    daily_forecast_tbl_comparison['Wday'] = daily_forecast_tbl_comparison['Day_week'].apply(lambda x :weekday_change[x] )

    daily_forecast_tbl_comparison = daily_forecast_tbl_comparison.merge(mean_two_weeks,left_on=['Wday','Terminal'],
                                        right_on=['Wday','Terminal'],how='left')
    daily_forecast_tbl_comparison = daily_forecast_tbl_comparison[~daily_forecast_tbl_comparison['Wday'].isin([1,7])]
    daily_forecast_tbl_comparison = daily_forecast_tbl_comparison[daily_forecast_tbl_comparison['Week'].isin(weeks)]
    daily_forecast_tbl_comparison = daily_forecast_tbl_comparison[daily_forecast_tbl_comparison['Year'].isin(year)]

    # Error : Forecast - Mean
    daily_forecast_tbl_comparison['Error'] = daily_forecast_tbl_comparison['Total Del Stops_x'] - daily_forecast_tbl_comparison['Total Del Stops_y']
    daily_forecast_tbl_comparison['% Error'] = daily_forecast_tbl_comparison['Total Del Stops_x'] / daily_forecast_tbl_comparison['Total Del Stops_y'] - 1


    daily_forecast_tbl_comparison = daily_forecast_tbl_comparison[(np.abs(daily_forecast_tbl_comparison['% Error'])>thr) & (np.abs(daily_forecast_tbl_comparison['Error'])>err)]
    return daily_forecast_tbl_comparison