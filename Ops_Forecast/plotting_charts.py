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



def make_df_ready_for_plotting(*df,table_names):
    modified_dataframes = []
    for idx,df in enumerate(df):
        modified_df = []
        modified_df = df.copy()
        modified_df = modified_df[['Date','Total Del Stops']]
        modified_df = modified_df.set_index('Date')
        modified_df.name = table_names[idx]
        modified_dataframes.append(modified_df)

    return modified_dataframes


def draw_line_charts(*dataframes,table_names):
    fig = px.line()
    colors = px.colors.qualitative.Plotly


    custom_colors = {
        'OpsForecast': 'black',
        'Test': 'red'
        }


    for idx, (df, table_name) in enumerate(zip(dataframes, table_names)):
        if table_name in custom_colors:
            line_color = custom_colors[table_name]
        else:
            line_color = colors[idx % len(colors)]  # Use different colors for other lines
        
        for column in df.columns:
            fig.add_scatter(x=df.index, y=df[column], mode='lines', 
                            name=f"Line {idx + 1} - {table_name}", 
                            line=dict(color=line_color, dash='dash' if table_name == 'OpsForecast' else 'solid'))
    
    fig.update_layout(title="Multiple Line Charts", xaxis_title="X-axis", yaxis_title="Y-axis")
    fig.show()


