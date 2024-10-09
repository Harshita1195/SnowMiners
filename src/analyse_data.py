import json
from typing import Any, Dict, List, Optional
import pandas as pd
import requests
import streamlit as st
import seaborn as sns
import matplotlib.pyplot as plt
import os
from os.path import join, dirname
#from dotenv import load_dotenv
import uuid
from utility import (
    get_snowflake_connection, 
    get_variables
)
from dotenv import load_dotenv
load_dotenv()

SF_DATABASE = os.getenv('SNOWFLAKE_DATABASE')  # Example: 'SFP_DB'
SF_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')  # Example: 'RAW'

def send_message(prompt: str) -> Dict[str, Any]:
    """Calls the REST API and returns the response."""
    env_variables = get_variables()
    request_body = {
        "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
        "semantic_model_file": f"@{env_variables['SF_DATABASE']}.{env_variables['SF_SCHEMA']}.{env_variables['SF_STAGE_CDP']}/{st.session_state.yaml_file}"
        #"semantic_model_file": f"@{env_variables['SF_DATABASE']}.{env_variables['SF_SCHEMA']}.{env_variables['SF_STAGE']}/{env_variables['SF_SEMANTIC_FILE_1']}",
    }
    resp = requests.post(
        url=f"https://{env_variables['SF_HOST']}/api/v2/cortex/analyst/message",
        json=request_body,
        headers={
            "Authorization": f'Snowflake Token="{st.session_state.CONN.rest.token}"',
            "Content-Type": "application/json",
        },
    )
    request_id = resp.headers.get("X-Snowflake-Request-Id")
    if resp.status_code < 400:
        return {**resp.json(), "request_id": request_id}  # type: ignore[arg-type]
    else:
        raise Exception(
            f"Failed request (id: {request_id}) with status {resp.status_code}: {resp.text}"
        )


def process_message(prompt: str) -> None:
    """Processes a message and adds the response to the chat."""
    st.session_state.messages.append(
        {"role": "user", "content": [{"type": "text", "text": prompt}]}
    )
    with st.chat_message("user"):
        st.markdown(prompt)
    with st.chat_message("assistant"):
        with st.spinner("Generating response..."):
            response = send_message(prompt=prompt)
            # print(response["message"]["content"][1]['statement'])
            request_id = response["request_id"]
            print(request_id)
            content = response["message"]["content"]
            display_content(content=content, request_id=request_id)  # type: ignore[arg-type]

    st.session_state.messages.append(
            {"role": "assistant", "content": content, "request_id": request_id}
    )


def build_visualization(request_id:str,
                         columns:str,
                         build_chart:bool,
                         data:Any=None
                         )->Any:
    i = 80000
    visualization_preference = {'graph_option': None,
                                'x_axis': None,
                                'y_axis': None,
                                'legend': None
                                }

    graph_option = st.selectbox('Please select the type of graph',
                                ("Line Chart", "Bar Chart", "Pie Chart"),
                                index=None,
                                key=f"{request_id}-{i}"
                                )
    i = i + 1
    legend = None
    visualization_preference['graph_option']=graph_option
    fig = plt.figure(figsize=(20, 4))
    visualization_preference['graph_type']=graph_option
    if graph_option == 'Line Chart':
        x_axis = st.selectbox('Please select the attribute for x axis', columns, index=None,
                              key=f"{request_id}-{i}")
        i = i + 1
        y_axis = st.selectbox('Please select the attribute for y axis', columns, index=None,
                              key=f"{request_id}-{i}")
        i = i + 1
        visualization_preference['x_axis'] = x_axis
        visualization_preference['y_axis'] = y_axis
        if len(columns) > 2:
            legend = st.selectbox('Please select the attribute for legend', columns, index=None,
                                  key=f"{request_id}-{i}")
            i = i + 1
            visualization_preference['legend'] = legend
        if x_axis and y_axis:
            if build_chart:
                sns.lineplot(x=x_axis, y=y_axis, hue=legend, data=data)
                return [fig,visualization_preference]
                #st.pyplot(fig)
            else:
                return visualization_preference
    elif graph_option == 'Bar Chart':
        x_axis = st.selectbox('Please select the attribute for x axis',
                              columns, index=None,
                              key=f"{request_id}-{i}"
                              )
        i = i + 1
        y_axis = st.selectbox('Please select the attribute for y axis', columns, index=None,
                              key=f"{request_id}-{i}")
        i = i + 1
        visualization_preference['x_axis'] = x_axis
        visualization_preference['y_axis'] = y_axis
        if len(columns) > 2:
            legend = st.selectbox('Please select the attribute for legend', columns, index=None,
                                  key=f"{request_id}-{i}")
            i = i + 1
            visualization_preference['legend'] = legend
        if x_axis and y_axis :
            if build_chart:
                sns.barplot(x=x_axis, y=y_axis, hue=legend, data=data)
                #st.pyplot(fig)
                return [fig, visualization_preference]
            else:
                return visualization_preference
    elif graph_option == 'Pie Chart':
        keys = st.selectbox('Please select the attribute for key', columns, index=None, key=f"{request_id}-{i}")
        i = i + 1
        values = st.selectbox('Please select the attribute for values', columns, index=None,
                              key=f"{request_id}-{i}")
        i = i + 1
        visualization_preference['x_axis'] = keys
        visualization_preference['y_axis'] = values
        if keys and values:
            if build_chart:
                plt.pie(data[values], labels=data[keys], autopct='%1.1f%%')
                return [fig, visualization_preference]
            else:
                return visualization_preference

def prompt_customer_preference(sql_generated:str,
                     user_query:str,
                     request_id:str,
                     columns:list,
                     chart_preference: dict=None
                     ) -> Any:
    i=70000
    campaing_config={}
    chart_required=None
    visualization_preference={}
    action= ['Save Analysis for Future Reference',
             'Convert Analysis to Campaign',
             'Both'
             ]
    action_selected = st.selectbox('Please select action to be performed', action, index=None, key=f"{request_id}-{i}")
    i = i+1
    if chart_preference:
        visualization_preference=chart_preference
        chart_required="Yes"

    if not (chart_preference) and action_selected in ('Save Analysis for Future Reference','Both'):
        chart_required = st.selectbox('Would you like to add visualization preference?', ['Yes','No'], index=None,
                                       key=f"{request_id}-{i}")
        i = i + 1
        if chart_required == 'Yes':
            visualization_preference=build_visualization(request_id,columns,False)
    if action_selected in ('Both','Convert Analysis to Campaign'):
        campaign_id=st.text_input("Please enter unique identifier for campaign",key=f"{request_id}-{i}")
        i = i + 1
        if campaign_id:
            campaign_target_system = st.selectbox('Please select campaign consumer',
                                                  ['Pega','m-platform','Internal'], index=None,
                                                  key=f"{request_id}-{i}"
                                                  )
            i = i + 1
            if campaign_target_system:
                campaign_frequency = st.selectbox('Please select campaign frequency', ['Daily', 'Monthly','Yearly','Quarterly','One-off'],
                                                      index=None,
                                                      key=f"{request_id}-{i}"
                                                  )
                campaing_config={'campaign_id':campaign_id,
                                 'campaign_target_system':campaign_target_system,
                                 'campaign_frequency':campaign_frequency
                                 }
                i = i + 1

    if action_selected:
        if action_selected == 'Save Analysis for Future Reference':
            if chart_required == 'Yes':
                if visualization_preference:
                    action_1=st.button("Submit Analysis",key=f"{request_id}-{i}")
                    i = i + 1
                    if action_1:
                        store_customer_preferences(user_query=user_query,
                                                   generated_sql=sql_generated,
                                                   user_action=action_selected,
                                                   chart_preference=chart_required,
                                                   chart_type=visualization_preference['graph_option'],
                                                   x_axis=visualization_preference['x_axis'],
                                                   y_axis=visualization_preference['y_axis'],
                                                   legend_key=visualization_preference['legend']
                                                   )
                        return True
                    i= i+1
            elif chart_required == 'No':
                action_2 = st.button("Submit",key=f"{request_id}-{i}")
                i = i + 1
                if action_2:
                    store_customer_preferences(user_query=user_query,
                                               generated_sql=sql_generated,
                                               user_action=action_selected,
                                               chart_preference=chart_required
                                               )
                    return True
        elif action_selected == 'Convert Analysis to Campaign':
            if campaing_config:
                action_3 = st.button("Build Campaign", key=f"{request_id}-{i}")
                i = i+1
                if action_3:
                    store_customer_preferences(user_query=user_query,
                                               generated_sql=sql_generated,
                                               user_action=action_selected,
                                               campaign_name=campaing_config['campaign_id'],
                                               campaign_consumer=campaing_config['campaign_target_system'],
                                               campaign_frequency=campaing_config['campaign_frequency']
                                               )
                    return True
        elif action_selected == 'Both':
            if chart_required == 'Yes':
                if visualization_preference and campaing_config:
                    action_4 = st.button("Build Campaign & Save Analysis", key=f"{request_id}-{i}")
                    i = i +1
                    if action_4:
                        store_customer_preferences(user_query=user_query,
                                                   generated_sql=sql_generated,
                                                   user_action=action_selected,
                                                   chart_preference=chart_required,
                                                   chart_type=visualization_preference['graph_option'],
                                                   x_axis=visualization_preference['x_axis'],
                                                   y_axis=visualization_preference['y_axis'],
                                                   legend_key=visualization_preference['legend'],
                                                   campaign_name=campaing_config['campaign_id'],
                                                   campaign_consumer=campaing_config['campaign_target_system'],
                                                   campaign_frequency=campaing_config['campaign_frequency']
                                                  )
                        return True
            else:
                if campaing_config:
                    action_5 = st.button("Build Campaign & Save Analysis", key=f"{request_id}-{i}")
                    if action_5:
                        store_customer_preferences(user_query=user_query,
                                                   generated_sql=sql_generated,
                                                   user_action=action_selected,
                                                   chart_preference=chart_required,
                                                   campaign_name=campaing_config['campaign_id'],
                                                   campaign_consumer=campaing_config['campaign_target_system'],
                                                   campaign_frequency=campaing_config['campaign_frequency']
                                                   )
                        return True

    return False

def store_customer_preferences(user_query,
                               generated_sql,
                               user_action,
                               chart_preference:str="No",
                               chart_type:str=None,
                               x_axis:str=None,
                               y_axis:str=None,
                               legend_key:str=None,
                               campaign_name:str=None,
                               campaign_consumer:str=None,
                               campaign_frequency:str=None
                               ):
    cs = st.session_state.CONN.cursor()
    env_variables = get_variables()
    cs.execute(f"call {env_variables['SF_DATABASE']}.{env_variables['SF_SCHEMA']}.capture_customer_preferences($${user_query}$$,$${generated_sql}$$,'{user_action}','{chart_preference}','{chart_type}','{x_axis}','{y_axis}','{legend_key}','{campaign_name}','{campaign_consumer}','{campaign_frequency}')")
    x = cs.fetchall()
    return x

def get_yaml_file():
    cs = st.session_state.CONN.cursor()
    env_variables = get_variables()
    cs.execute(f"call {env_variables['SF_DATABASE']}.{env_variables['SF_SCHEMA']}.get_yaml('abc')")
    x = cs.fetchall()
    yaml_tuple=x[0]
    yaml_list=yaml_tuple[0].split(',')
    return yaml_list

def display_content(
        content: List[Dict[str, str]],
        request_id: Optional[str] = None,
        message_index: Optional[int] = None,
) -> None:
    """Displays a content item for a message."""
    message_index = message_index or len(st.session_state.messages)
    count = False
    i = 50000
    if request_id:
        with st.expander("Request ID", expanded=False):
            st.markdown(request_id)
    for item in content:
        if item["type"] == "user_response":
            st.markdown(item["text"])
        if item["type"] == "text":
            user_query=item["text"]
            st.markdown(user_query)
        elif item["type"] == "suggestions":
            with st.expander("Suggestions", expanded=True):
                for suggestion_index, suggestion in enumerate(item["suggestions"]):
                    if st.button(suggestion, key=f"{message_index}_{suggestion_index}"):
                        st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            with st.expander("SQL Query", expanded=False):
                st.code(item["statement"], language="sql")
            with st.expander("Results", expanded=True):
                with st.spinner("Running SQL..."):
                    sql_generated=item["statement"]
                    df = pd.read_sql(sql_generated, st.session_state.CONN)
                    columns = list(df.columns)
                    if len(df.index) > 1:
                        count = True
                    st.dataframe(df)
    if count:
        if request_id:
            get_graph = st.radio(
                "Would you like to view the output as a graph?",
                ["No", "Yes"],
                captions=[
                    "No , thanks..",
                    "Yes Please"
                ],
                index=None,
                key=f"{request_id}-{i}"
            )
            i = i + 1
            if get_graph == "Yes":
                visualization_data = build_visualization(request_id, columns, True,df)
                if visualization_data:
                    visualization_preference=visualization_data[1]
                    st.pyplot(visualization_data[0])
                    ## Save Analysis
                    save_sql = st.radio(
                        "Would you like to save the analysis for future ?",
                        ["No", "Yes"],
                        captions=[
                            "No , thanks..",
                            "Yes Please"
                        ],
                        index=None,
                        key=f"{request_id}-{i}"
                    )
                    i = i + 1
                    if save_sql == 'Yes':
                        save_sql_stat = prompt_customer_preference(sql_generated=sql_generated,
                                                                   user_query=user_query,
                                                                   request_id=request_id,
                                                                   columns=columns,
                                                                   chart_preference=visualization_preference
                                                                   )
                        if save_sql_stat:
                            st.write("Request saved  successfully..")
            else:
                st.write("Please explore..")
    else:
        if request_id:
            save_sql = st.radio(
                "Would you like to save the analysis for future ?",
                ["No", "Yes"],
                captions=[
                    "No , thanks..",
                    "Yes Please"
                ],
                index=None,
                key=f"{request_id}-{i}"
            )
            i = i+1
            if save_sql=='Yes':
                save_sql_stat = prompt_customer_preference(sql_generated,user_query,request_id,columns)
                if save_sql_stat:
                    st.write("Analysis Saved ..")

