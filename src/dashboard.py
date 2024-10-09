import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from utility import get_variables
def create_line_chart(df, x_column, y_column):
    """
    Function to create a line chart using Streamlit and Matplotlib.
    
    Parameters:
    df (pd.DataFrame): The DataFrame containing the data.
    x_column (str): The column name to be used for the X-axis.
    y_column (str): The column name to be used for the Y-axis.
    """
    # Create a line plot
    fig, ax = plt.subplots()
    ax.plot(df[x_column], df[y_column], marker='o')

    # Set chart title and labels
    ax.set_title(f'{y_column} over {x_column}')
    ax.set_xlabel(x_column)
    ax.set_ylabel(y_column)

    return fig

    # Display the chart in Streamlit
    # st.pyplot(fig)

def build_pie_chart(df:any,
                    values:str,
                    keys:str,
                    title:str=None
                    ):
    fig, ax = plt.subplots(figsize=(6, 4))
    plt.pie(df[values], labels=df[keys], autopct='%1.1f%%')
    ax.set_title(title, weight='bold')
    return fig

def build_line_chart(df:any,
                    x_axis:str,
                    y_axis:str,
                    legend:str=None,
                    title:str=None
                    ):
    fig, ax = plt.subplots(figsize=(6, 4))
    sns.lineplot(x=x_axis, y=y_axis, hue=legend, data=df)
    ax.set_title(title, weight='bold')
    ax.tick_params(axis='x', rotation=45)
    return fig

def build_bar_chart(df:any,
                    x_axis:str,
                    y_axis:str,
                    legend:str=None,
                    title:str=None
                    ):
    fig, ax = plt.subplots(figsize=(6, 4))
    sns.barplot(x=x_axis, y=y_axis, hue=legend, data=df)
    ax.set_title(title, weight='bold')
    ax.tick_params(axis='x', rotation=45)
    return fig

def get_dashboard_data():
    data = {
        'Date': ['2024-10-01', '2024-10-02', '2024-10-03', '2024-10-04', '2024-10-05'],
        'Sales': [100, 150, 200, 130, 170]
    }
    
    df = pd.DataFrame(data)
    df['Date'] = pd.to_datetime(df['Date'])  # Convert to datetime

    return df

def get_data_using_sql(ip_key:str=None,
                       sql:str=None
                      ):
    if not ip_key:
        ip_key='default'

    snow_db = get_variables()['SF_DATABASE']
    snow_schema = get_variables()['SF_SCHEMA']

    sql_dict={}
    sql_dict['usage_type']=f"Select user_action, count(1) as request_count from {snow_db}.{snow_schema}.saved_queries group by user_action;"
    sql_dict['campaign_consumer'] = f"Select campaign_consumer, count(1) as campaign_count from {snow_db}.{snow_schema}.saved_queries where campaign_consumer is not null group by campaign_consumer;"
    sql_dict['top_sql'] = f"Select * from {snow_db}.{snow_schema}.top_three_sql;"
    sql_dict['default'] = f"Select * from {snow_db}.{snow_schema}.top_three_sql;"
    if not sql and ip_key:
       sql = sql_dict[ip_key]

    df = pd.read_sql(sql, st.session_state.CONN)
    print(df.head())
    return df

def build_chart_for_top3(df):
    fig_list=[]
    for i, row in df.iterrows():
        title=row['TITLE']
        generated_sql=row['GENERATED_SQL']
        chart_type=row['CHART_TYPE']
        x_axis = row['X_AXIS']
        y_axis = row['Y_AXIS']
        legend_key= row['LEGEND_KEY']
        if legend_key:='None':
            legend_key=None
        data=get_data_using_sql(sql=generated_sql)
        if chart_type=='Line Chart':
            fig = build_line_chart(df=data,
                             x_axis=x_axis,
                             y_axis=y_axis,
                             legend=legend_key,
                             title=title
                             )
        elif chart_type=='Bar Chart':
            fig = build_bar_chart(df=data,
                                   x_axis=x_axis,
                                   y_axis=y_axis,
                                   legend=legend_key,
                                   title=title
                                   )
        elif chart_type=='Pie Chart':
            fig = build_pie_chart(df=data,
                                   values=y_axis,
                                   keys=x_axis,
                                   title=title
                                   )
        fig_list.append(fig)
    return fig_list


