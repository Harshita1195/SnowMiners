import streamlit as st
import pandas as pd
from streamlit_option_menu import option_menu


import streamlit as st
import os
import pandas as pd
from snowflake import connector
from langchain.text_splitter import CharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
from langchain_openai import ChatOpenAI
from langchain.vectorstores import FAISS
from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate
from langchain.document_loaders import PyPDFLoader
from dotenv import load_dotenv


from typing import Any, Dict, List, Optional
import requests
import seaborn as sns
import matplotlib.pyplot as plt
import os
from dashboard import create_line_chart, get_dashboard_data,get_data_using_sql,build_pie_chart,build_chart_for_top3
from utility import get_snowflake_connection, get_variables, create_connection, get_snowflake_connection_analyze
import base64
from datetime import datetime
import streamlit as st
import pandas as pd
from streamlit_option_menu import option_menu

from typing import Any, Dict, List, Optional
import requests
import seaborn as sns
import matplotlib.pyplot as plt
import os
import base64
from admin_apps.journeys import builder, iteration, partner

from dashboard import create_line_chart, get_dashboard_data
from utility import (
    get_snowflake_connection, 
    get_variables
)
from analyse_data import (
    send_message, 
    process_message, 
    build_visualization,
    prompt_customer_preference,
    store_customer_preferences,
    get_yaml_file,
    display_content,
)

from admin_apps.shared_utils import (  # noqa: E402
    GeneratorAppScreen
)
from doc_chat import (
    generate_embeddings,
    fetch_document_chunks,
    fetch_files,
    create_prompt,
    complete,
    check_embeddings_exist
)
from dotenv import load_dotenv
load_dotenv()

# STYLING START

# Set the page configuration (optional)
st.set_page_config(page_title="Snow Miners", layout="wide", page_icon="assets/favicon.ico")

SF_DATABASE = os.getenv('SNOWFLAKE_DATABASE')  # Example: 'SFP_DB'
SF_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')  # Example: 'RAW'


# # Folder where PDFs are stored
# BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Get the directory of the current script
# PDF_FOLDER = os.path.join(BASE_DIR, "data/pdf_files/SFP")       # Combine with the 'pdf_files' folder

# Folder where PDFs are stored
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Go up one level from 'src'
PDF_FOLDER = os.path.join(BASE_DIR, "data/pdf_files/ESG")  # Combine with the 'data/pdf_files/SFP' folder


st.write('<style>div.block-container{padding-top:2rem;}</style>', unsafe_allow_html=True)

st.markdown("""
    <style>
    .right-button {
        display: inline-block;
        float: right;
        padding: 10px 20px;
        font-size: 16px;
        color: white;
        background-color: #4CAF50;
        border: none;
        border-radius: 10px;
        text-align: center;
        text-decoration: none;
        cursor: pointer;
        font-weight: bold;
    }
    .right-button:hover {
        background-color: #45a049;
    }
    </style>
    """, unsafe_allow_html=True)

# Load CSS
with open("style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# STYLING END

if 'cur_extract_section' not in st.session_state:
    st.session_state.current_section = 1  # Start at section 1

# Initialize session state for selected row
if 'selected_dc_yaml_file' not in st.session_state:
    st.session_state.selected_dc_yaml_file = None

if 'selected_row_dc_sm_model' not in st.session_state:
    st.session_state.selected_row_dc_sm_model = None

if 'selected_row_review' not in st.session_state:
    st.session_state.selected_row_review = None

 # Initialize session state for selected row
if 'selected_row_dc_all' not in st.session_state:
    st.session_state.selected_row_dc_all = None

if 'CONN' not in st.session_state or st.session_state.CONN is None:
    config = get_variables()
    st.session_state.CONN = get_snowflake_connection_analyze(config)
else:
    conn = st.session_state.CONN

if "messages" not in st.session_state:
    st.session_state.messages = []
    st.session_state.suggestions = []
    st.session_state.active_suggestion = None
    st.session_state.yaml_file= None

# Populating common state between builder and iteration apps.
st.session_state["account_name"] = os.getenv('SNOWFLAKE_ACCOUNT_LOCATOR')
st.session_state["host_name"] = os.getenv('SNOWFLAKE_HOST')
st.session_state["user_name"] = os.getenv('SNOWFLAKE_USER')


def list_files_in_directory(directory_path):
    # List files in the directory
    files = []
    for file_name in os.listdir(directory_path):
        file_path = os.path.join(directory_path, file_name)
        if os.path.isfile(file_path):
            files.append({"File Name": file_name, "File Path": file_path})
    return files

# Function to go to the next section
def next_section():
    if st.session_state.current_section < 4:
        st.session_state.current_section += 1

# Function to go to the previous section
def prev_section():
    if st.session_state.current_section > 1:
        st.session_state.current_section -= 1

# Function to list all PDFs in the folder
def list_pdfs(pdf_folder):
    # Check if the folder exists
    if not os.path.exists(pdf_folder):
        st.error(f"The folder '{pdf_folder}' does not exist.")
        return []
    
    # List the PDF files in the folder
    return [f for f in os.listdir(pdf_folder) if f.endswith('.pdf')]

def get_snowflake_stages():
    query = f"""
    SELECT stage_name, stage_schema, stage_type
    FROM {SF_DATABASE}.INFORMATION_SCHEMA.STAGES;
    """

    # Execute the query
    conn = st.session_state.CONN
    cur = conn.cursor()
    cur.execute(query)
    
    # Fetch all stage names
    stages = cur.fetchall()
    
    # Extract the stage names from the result
    stage_names = [stage[0] for stage in stages]
    
    return stage_names

# Function to get column names from a Snowflake table
def get_snowflake_table_columns(table_name):
    query = f"""
        SELECT COLUMN_NAME
        FROM {SF_DATABASE}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{SF_SCHEMA}'
        AND TABLE_NAME = '{table_name}';
    """

    conn = st.session_state.CONN
    cur = conn.cursor()
    cur.execute(query)
    
    # Fetch all column names
    columns = cur.fetchall()
    
    # Extract column names into a list
    column_list = [col[0] for col in columns]
    
    return column_list

def escape_sql_value(value):
    if isinstance(value, str):
        return value.replace("'", "''")  # Replace single quotes with two single quotes
    return value  # Return the value as is if it's not a string
# Function to read the PDF file and encode it to base64
def pdf_to_base64(file_path):
    with open(file_path, "rb") as pdf_file:
        base64_pdf = base64.b64encode(pdf_file.read()).decode('utf-8')
    return base64_pdf

def show_charts():
    col1, col2 = st.columns(2)

    usage_data=get_data_using_sql(sql=None,ip_key='usage_type')
    campaign_consumer=get_data_using_sql(sql=None,ip_key='campaign_consumer')
    top3_sql=get_data_using_sql(sql=None,ip_key='top_sql')

    top3_fig_list=build_chart_for_top3(df=top3_sql)

    with col1:
        #fig = create_line_chart(df, x_column='Date', y_column='Sales')
        fig =build_pie_chart(df=usage_data,
                             values='REQUEST_COUNT',
                             keys='USER_ACTION',
                             title='Volume of User Requests vs Action Type'
                             )
        st.pyplot(fig)

    with col2:
        fig = build_pie_chart(df=campaign_consumer,
                              values='CAMPAIGN_COUNT',
                              keys='CAMPAIGN_CONSUMER',
                              title='Campaign Vs Consumers'
                              )
        if fig:
            st.pyplot(fig)
        else:
            pass

    col1, col2 ,col3 = st.columns(3)

    with col1:
        if len(top3_fig_list)>=2:
            st.pyplot(top3_fig_list[1])
        else:
            pass

    with col2:
        if len(top3_fig_list) >= 3:
            st.pyplot(top3_fig_list[2])
        else:
            pass

    with col3:
        if len(top3_fig_list) >= 1:
            st.pyplot(top3_fig_list[0])
        else:
            pass




# Function to get the max JOB_ID from the JOB table
def get_max_job_id():
    query = f"SELECT MAX(JOB_ID) FROM {SF_DATABASE}.{SF_SCHEMA}.JOB;"
    
    cur = conn.cursor()
    cur.execute(query)
    
    # Fetch the max JOB_ID
    max_job_id = cur.fetchone()[0]
    
    # If no JOB_ID exists, start with 1
    if max_job_id is None:
        max_job_id = 0
    
    return max_job_id

# Function to insert a new row into the JOB table
def insert_job(new_job_id, extract_type, model, status):
    query = f"""
    INSERT INTO {SF_DATABASE}.{SF_SCHEMA}.JOB (JOB_ID, EXTRACT_TYPE, MODEL, STATUS, CREATED_DATE, MODIFIED_DATE)
    VALUES ({new_job_id}, '{extract_type}', '{model}', '{status}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
    """
    
    cur = st.session_state.CONN.cursor()
    cur.execute(query)
    conn.commit()

# Function to insert multiple rows into the JOB_DETAIL table
def insert_job_details(new_job_id, ls_attributes, ls_prompts):
    cur = conn.cursor()

    if ls_prompts is None:
        ls_prompts = [None] * len(ls_attributes)

    for attribute, prompt in zip(ls_attributes, ls_prompts):
        jobId = get_max_job_id()
        query = f"""
        INSERT INTO {SF_DATABASE}.{SF_SCHEMA}.JOB_DETAIL (JOB_ID, ATTRIBUTE, PROMPT)
        VALUES ({new_job_id}, '{attribute}', '{prompt}');
        """
        st.write(query)
        cur.execute(query)

    conn.commit()

def show_start_content():

    st.title("Snow Miners: Make Data Work for You")

    st.write("""
    Working with data doesn’t have to be complicated. **Snow Miners** makes it easy to **extract**, **review**, **analyze**, and **share** your data, all in one place. Whether you’re handling big datasets or just need quick insights, we’ve got you covered.
    """)

    st.subheader("🚀 Extract Data with Precision")
    st.write("""
    With **Snow Miners**, you can quickly pull data from anywhere—databases, APIs, or cloud storage—with just a few clicks. No more manual work, just fast results.
    """)

    st.subheader("👀 Review Data, No Hassle")
    st.write("""
    Easily review and clean your data. Our intuitive interface lets you spot errors, make corrections, and ensure your data is always accurate, all in real time.
    """)

    st.subheader("📊 Analyze Like a Pro")
    st.write("""
    Run powerful analyses and create interactive reports without the need for complex tools. **Snow Miners** helps you uncover trends and insights with ease.
    """)

    st.subheader("🌍 Share MetaData Effortlessly")
    st.write("""
    Collaboration is simple. Export your data, share live reports, and integrate with cloud platforms to keep your team in the loop.
    """)

    st.markdown("**Ready to Simplify Your Data?**")
    st.write("""
    Try **Snow Miners** today and see how easy data management can be.
    """)



# Define the sidebar and option menu
with st.sidebar:
    st.image("assets/logo.png", width=200)
    menu_selection = option_menu(
        menu_title="Snow Miners",
        options=["Introduction","Dashboard", 'Extract', 'Review', 'Analyze', 'Data Catalogue'],
        icons=["house", "file-earmark-text", "clipboard-check", "bar-chart", "shop-window"],
        menu_icon="menu-button",
        default_index=0,
        styles={
            "container": {"padding": "0!important", "background-color": "#f0f0f5"},
            "icon": {"color": "black", "font-size": "18px"},
            "nav-link": {"font-size": "14px", "text-align": "left", "margin": "0px", "--hover-color": "#e8e8ef"},
            "nav-link-selected": {"background-color": "#6c757d"},
        }
    )

with st.container():

    if menu_selection == "Introduction":  # Only display when "Extract" is selected
        

        enable_dashboard = False

        if enable_dashboard:
            st.markdown(
            """
            <h1 style="text-align: left; color: #ee775e; font-size: 2.5em; margin: 0; margin-top: 0;">
                Dashboard
            </h1>
            <hr style="border: 2px solid #ee775e; width: 100%; margin: 10px auto;">
            """, unsafe_allow_html=True
        ) 

            df = get_dashboard_data()

            show_charts()
        else:
           show_start_content()

    elif menu_selection == "Dashboard":  # Only display when "Extract" is selected

        enable_dashboard = True

        if enable_dashboard:
            st.markdown(
                """
                <h1 style="text-align: left; color: #ee775e; font-size: 2.5em; margin: 0; margin-top: 0;">
                    Dashboard
                </h1>
                <hr style="border: 2px solid #ee775e; width: 100%; margin: 10px auto;">
                """, unsafe_allow_html=True
            )

            df = get_dashboard_data()

            show_charts()
        else:
            show_start_content()

    elif menu_selection == "Extract":  # Only display when "Extract" is selected

        st.title('Extract')

        tab_Extract, tab_Model, tab_File, tab_Attribute, tab_Review_Create = st.tabs(["Extract Type", "Model Selection", "Files", "Define Attributes", "Review"])

        #User Mortgage Data Tab
        with tab_Extract:

            # Create a single radio button for all sub-container options
            radio_options = ["Single Values", "Multi Text Values", "Combined Multi Text Values"]
            rbtn_extract_type = st.radio("**Select a value type:**", radio_options)

            # Create three columns for the sub-containers
            col_single, col_text, col_combined = st.columns(3)  # Create 3 equal-width columns
            
            # Create the first sub-container with borders in the first column
            with col_single:
                st.markdown(
                f"""
                <div style="color:white;border: 2px solid #29B5E8; border-radius: 5px; padding: 10px; margin: 5px; height: 400px; background-color: #29B5E8;">
                    <div>
                        <p><b>❄ SINGLE VALUES</b></p>
                        <p> Int: 10 (Loan Tenure)</p>
                        <p> Text: ABS158469 (Company House Number)</p>
                        <p> Date: 2023-04-23 (Loan Completion Date)</p>
                        <p> Bool: False (Is data confidential)</p>
                    </div>
                </div>
                """, unsafe_allow_html=True
            )

            # Create the second sub-container with borders in the second column
            with col_text:
                st.markdown(
                    f"""
                    <div style="color:white;border: 2px solid #29B5E8; border-radius: 5px; padding: 10px; margin: 5px; height: 400px; background-color: #29B5E8;">
                        <div>
                            <p><b>❄ MULTI TEXT VALUES</b></p>
                            <p>Target project categories:</p>
                            <p>1. Education & Employment Promotion</p>
                            <p>2. Socio-economic advancement & empowerment</p>
                            <p>3. Affordable housing</p>
                            <p>4. Access to basic infrastructure</p>
                        </div>
                    </div>
                    """, unsafe_allow_html=True
                )
            
            # Create the third sub-container with borders in the third column
            with col_combined:
                st.markdown(
                    f"""
                    <div style="color:white;border: 2px solid #29B5E8; border-radius: 5px; padding: 10px; margin: 5px; height: 400px; background-color: #29B5E8;">
                        <div>
                            <p><b>❄ COMBINED MULTI TEXT VALUES</b></p>
                            <p> Int: 10(Loan Tenure)</p>
                            <p> Text: ABS158469(Company House Number)</p>
                            <p> Date: 2023-04-23(Loan Completion Date)</p>
                            <p>1. Education & Employment Promotion</p>
                            <p>2. Socio-economic advancement & empowerment</p>
                            <p>3. Affordable housing</p>
                        </div>
                    </div>
                    """, unsafe_allow_html=True
                )

        if rbtn_extract_type=="Single Values":
            
            with tab_Model:
                selected_model = st.selectbox("**Select Model**", ["DOC_AI_Company_House",'mixtral-8x7b', 'snowflake-arctic', 'mistral-large'])
                st.write(f"Recommended Model: DOC_AI_Company_House")
                st.markdown("[Create your own Model](https://app.snowflake.com/vpfboih/ql38210/#/document-ai)")
                
                # Create a new row for the Next button at the bottom right
                st.markdown("<br>", unsafe_allow_html=True)  # Add some spacing
                col_space, col_next = st.columns([8, 1])  # 8 for space, 1 for the Next button

            with tab_File:
                st.write("**Upload your file:**")
                url = "https://app.snowflake.com/vpfboih/ql38210/#/data/databases/ESG_DB/schemas/RAW/stage/STG_COMPANY_HOUSE"  # Replace with your desired URL
                st.markdown(
                    f'<a href="{url}" target="_blank">'
                    f'<button style="background-color: #ee775e; color: white; padding: 5px 10px; font-size: 12px; border: none; border-radius: 4px; cursor: pointer;">'
                    f'Upload your files'
                    f'</button></a>',
                    unsafe_allow_html=True
                )
                # st.markdown("[https://app.snowflake.com/vpfboih/ql38210/.....](https://app.snowflake.com/vpfboih/ql38210/#/data/databases/SNOW_DB/schemas/SNOW_SCHEMA/stage/COMPANY_HOUSE_FILES_STAGE)")
                
                # # Query to get the list of stages
                # conn= get_snowflake_connection()
                # query = """
                # LS @company_house_files_stage; """
                # # LS @snow_db.snow_schema.company_house_files_stage; """

                # # Execute the query and fetch the results
                # cur = conn.cursor()
                # cur.execute(query)
                # stages = cur.fetchall()
                
                # # Extract only the stage names (second element in each tuple)
                # stage_names = [stage[0].split('/')[-1] for stage in stages]
                
                # # Convert stage_names into a DataFrame and rename the column to "Available Files"
                # df_stage_names = pd.DataFrame(stage_names, columns=["Available Files"])

                if st.button("Fetch Files"):
                    
                    file_list = list_files_in_directory(PDF_FOLDER)

                    # Create a DataFrame to display the filenames in a table
                    df = pd.DataFrame(file_list)

                    df['File Path'] = df['File Path'].replace(
                        '/Users/aditya/CodingPractice/SM_ESG/pdf_files/ESG/',
                        'https://app.snowflake.com/vpfboih/ql38210/#/data/databases/ESG_DB/schemas/RAW/stage/STG_COMPANY_HOUSE',
                        regex=True
                    )
                    # Customize the dataframe size by setting width and height
                    st.dataframe(df, width=700, height=300)

            with tab_Attribute:
                if selected_model== 'DOC_AI_Company_House':
                    st.warning(f'''Since you have selected trained model: {selected_model}, 
                                 attributes are already set at the time of training DOC AI.
                               Below are the attributes available to extract.''')
                    values =get_snowflake_table_columns('EXTRACTED_DATA_DOC_AI')
                    # List of items to remove
                    items_to_remove = ['REPORTING_DATE', 'URL', 'LAST_MODIFIED', 'FILE_NAME', 'FEEDBACK', 'JOB_ID', 'FILE_SIZE']

                    # Removing the specified items from the list
                    filtered_values = [item for item in values if item not in items_to_remove]

                    # Convert stage_names into a DataFrame and rename the column to "Available Files"
                    df_attributes = pd.DataFrame(filtered_values, columns=["Attributes to Extract"])

                    # Customize the dataframe size by setting width and height
                    st.dataframe(df_attributes, width=700, height=300)
                else:
                    st.write('Other model selected!')

            with tab_Review_Create:
                    
                st.write(f"**Selected Model**: {selected_model}")
                
                extraction_type =rbtn_extract_type

                file_path ="https://app.snowflake.com/vpfboih/ql38210/#/data/databases/ESG_DB/schemas/RAW/stage/STG_COMPANY_HOUSE"
                
                st.write("**File path**:[https://app.snowflake.com/vpfboih/ql38210/.....](https://app.snowflake.com/vpfboih/ql38210/#/data/databases/ESG_DB/schemas/RAW/stage/STG_COMPANY_HOUSE)")
                
                attributes_to_extract =get_snowflake_table_columns('EXTRACTED_DATA_DOC_AI')

                # List of items to remove
                items_to_remove = ['REPORTING_DATE', 'URL', 'LAST_MODIFIED', 'FILE_NAME', 'FEEDBACK', 'JOB_ID', 'FILE_SIZE']

                # Removing the specified items from the list
                filtered_attributes = [item for item in attributes_to_extract if item not in items_to_remove]

                # Convert stage_names into a DataFrame and rename the column to "Available Files"
                df_attributes = pd.DataFrame(filtered_attributes, columns=["Attributes to Extract"])

                # Customize the dataframe size by setting width and height
                st.dataframe(df_attributes, width=700, height=300)


                radio_freq = ["On-Demand", "Recurring"]
                radio_selections = st.radio("**Extract Frequency**", radio_freq)

                if radio_selections == 'Recurring':
                    cron_exp = st.text_input("Enter CRON expression")

                ls_stages = get_snowflake_stages()

                extract_location = st.selectbox("**Extract Location**", ls_stages)
                
                if st.button("Create Job"):
                    # Get the max JOB_ID and calculate the new JOB_ID
                    max_job_id = get_max_job_id()
                    new_job_id = max_job_id + 1

                    # Insert the new job
                    insert_job(new_job_id, extraction_type, selected_model, 'SCHEDULED')

                    # Insert multiple job details
                    insert_job_details(new_job_id, filtered_attributes, None)

                    st.success(f"Job is submitted successfully!")
                    # save_single_value_data()
                    # conn= get_snowflake_connection()
                    # query = f"""
                    # INSERT INTO job_table (Job_Name, extraction_type, Selected_Model, File_Path) 
                    # VALUES ('{job_name}', '{extraction_type}','{selected_model}', '{file_path}' );"""

                    # # Execute the query and fetch the results
                    # query_2 = """COPY INTO @model_extracts_stage/company_house_extracts.csv
                    # FROM (
                    #     SELECT file_name, current_asset, fixed_asset, debt, company_name, chn, reporting_date, equity, revenue
                    #     FROM extracted_data_view
                    # )
                    # FILE_FORMAT = my_csv_format
                    # OVERWRITE = TRUE
                    # SINGLE = TRUE;"""

                    # cur = conn.cursor()
                    # cur.execute(query)
                    # cur.execute(query_2)
                    # st.success("Job created successfully!")  

        elif rbtn_extract_type=="Multi Text Values":
            ls_attributes = []
            ls_prompts = []
            
            with tab_Model:
                selected_model = st.selectbox("**Select Model**", ['mixtral-8x7b', 'snowflake-arctic', 'mistral-large', 
                                                            'llama3-8b', 'llama3-70b', 'reka-flash',
                                                            'mistral-7b', 'llama2-70b-chat', 'gemma-7b'])
                st.write(f"Recommended Model: mixtral-8x7b")
                
            with tab_File:
                st.write("**Upload your file:**")
                url = "https://app.snowflake.com/auvfake/gc59694/#/data/databases/SFP_DB/schemas/RAW/stage/STG_LOAN_AGREEMENTS"  # Replace with your desired URL
                st.markdown(
                    f'<a href="{url}" target="_blank">'
                    f'<button style="background-color: #ee775e; color: white; padding: 5px 10px; font-size: 12px; border: none; border-radius: 4px; cursor: pointer;">'
                    f'Upload your files'
                    f'</button></a>',
                    unsafe_allow_html=True
                )

                if st.button("Fetch Files"):
                    
                    file_list = list_files_in_directory(PDF_FOLDER)

                    # Create a DataFrame to display the filenames in a table
                    df = pd.DataFrame(file_list)

                    df['File Path'] = df['File Path'].replace(
                        '/Users/aditya/CodingPractice/SM_SFP/pdf_files/SFP/',
                        'https://app.snowflake.com/auvfake/gc59694/#/data/databases/SFP_DB/schemas/RAW/stage/STG_LOAN_AGREEMENTS',
                        regex=True
                    )
                    # Customize the dataframe size by setting width and height
                    st.dataframe(df, width=700, height=300)

            with tab_Attribute:
                key_col1, val_col2 = st.columns(2)
                with key_col1:
                    user_k1 = st.text_input("Attribute 1:")
                    user_k2 = st.text_input("Attribute 2:")
                    user_k3 = st.text_input("Attribute 3:")
                    user_k4 = st.text_input("Attribute 4:")
                    user_k5 = st.text_input("Attribute 5:")

                with val_col2:
                    user_p1 = st.text_input("Prompt 1:")
                    user_p2 = st.text_input("Prompt 2:")
                    user_p3 = st.text_input("Prompt 3:")
                    user_p4 = st.text_input("Prompt 4:")
                    user_p5 = st.text_input("Prompt 5:")

                                
                ls_attributes = [k for k in [user_k1, user_k2, user_k3, user_k4, user_k5] if k]
                ls_prompts = [p for p in [user_p1, user_p2, user_p3, user_p4, user_p5] if p]


            with tab_Review_Create:
                st.write(f"**Selected Model**: {selected_model}")
                

                file_path ="https://app.snowflake.com/vpfboih/ql38210/#/data/databases/SNOW_DB/schemas/SNOW_SCHEMA/stage/COMPANY_HOUSE_FILES_STAGE"
                
                st.write("**File path**:[https://app.snowflake.com/auvfake/gc59694/.....](hhttps://app.snowflake.com/auvfake/gc59694/#/data/databases/SFP_DB/schemas/RAW/stage/STG_LOAN_AGREEMENTS)")
                
                # Create a DataFrame with two columns
                df_attributes = pd.DataFrame({
                    'Attributes': ls_attributes,
                    'Prompts': ls_prompts
                })

                # Customize the dataframe size by setting width and height
                st.dataframe(df_attributes)


                radio_freq = ["On-Demand", "Recurring"]
                radio_selections = st.radio("**Extract Frequency**", radio_freq)

                if radio_selections == 'Recurring':
                    cron_exp = st.text_input("Enter CRON expression")

                ls_stages = get_snowflake_stages()

                extract_location = st.selectbox("**Extract Location**", ls_stages)
                
                if st.button("Create Job"):
                    # Get the max JOB_ID and calculate the new JOB_ID
                    max_job_id = get_max_job_id()

                    print(max_job_id)
                    new_job_id = max_job_id + 1

                    # Insert the new job
                    insert_job(new_job_id, rbtn_extract_type, selected_model, 'SCHEDULED')

                    # Insert multiple job details
                    insert_job_details(new_job_id, ls_attributes, ls_prompts)

                    st.success(f"Job is submitted successfully!")

        elif rbtn_extract_type=="Combined Multi Text Values":
            with tab_Model:
                model = st.selectbox("**Select Model**", ["DOC_AI_CompanyHouse","DOC_AI_Treasury",'mixtral-8x7b', 'snowflake-arctic', 'mistral-large', 
                                                            'llama3-8b', 'llama3-70b', 'reka-flash',
                                                            'mistral-7b', 'llama2-70b-chat', 'gemma-7b'])
                st.write(f"Selected Model: {model}")
                
                # Create a new row for the Next button at the bottom right
                st.markdown("<br>", unsafe_allow_html=True)  # Add some spacing
                col_space, col_next = st.columns([8, 1])  # 8 for space, 1 for the Next button

            with tab_File:
                st.write("**Upload your file:**")
                url = "https://app.snowflake.com/vpfboih/ql38210/#/data/databases/SNOW_DB/schemas/SNOW_SCHEMA/stage/LOAN_AGREEMENTS_STAGE"  # Replace with your desired URL
                st.markdown(
                    f'<a href="{url}" target="_blank">'
                    f'<button style="background-color: #ee775e; color: white; padding: 5px 10px; font-size: 12px; border: none; border-radius: 4px; cursor: pointer;">'
                    f'Upload your files'
                    f'</button></a>',
                    unsafe_allow_html=True
                )
                st.markdown("[https://app.snowflake.com/vpfboih/ql38210/.....](https://app.snowflake.com/vpfboih/ql38210/#/data/databases/SNOW_DB/schemas/SNOW_SCHEMA/stage/LOAN_AGREEMENTS_STAGE)")
                
                # Query to get the list of stages
                conn= get_snowflake_connection()
                query = """
                LS @LOAN_AGREEMENTS_STAGE; """

                # Execute the query and fetch the results
                cur = conn.cursor()
                cur.execute(query)
                stages = cur.fetchall()
                
                # Extract only the stage names (second element in each tuple)
                stage_names = [stage[0].split('/')[-1] for stage in stages]
                
                # Convert stage_names into a DataFrame and rename the column to "Available Files"
                df_stage_names = pd.DataFrame(stage_names, columns=["Available Files"])

                # Customize the dataframe size by setting width and height
                st.dataframe(df_stage_names, width=700, height=300)

            with tab_Attribute:
                key_col1, val_col2 = st.columns(2)
                with key_col1:
                    user_k1 = st.text_input("Key 1:")
                    user_k2 = st.text_input("Key 2:")
                    user_k3 = st.text_input("Key 3:")
                    user_k4 = st.text_input("Key 4:")
                    user_k5 = st.text_input("Key 5:")

                with val_col2:
                    user_p1 = st.text_input("Prompt 1:")
                    user_p2 = st.text_input("Prompt 2:")
                    user_p3 = st.text_input("Prompt 3:")
                    user_p4 = st.text_input("Prompt 4:")
                    user_p5 = st.text_input("Prompt 5:")

            with tab_Review_Create:
                    
                st.write(f"**Selected Model**: mixtral-8x7b")
                
                selected_model="mixtral-8x7b"

                file_path ="https://app.snowflake.com/vpfboih/ql38210/#/data/databases/SNOW_DB/schemas/SNOW_SCHEMA/stage/LOAN_AGREEMENTS_STAGE"
                
                st.write("**File path**:[https://app.snowflake.com/vpfboih/ql38210/.....](https://app.snowflake.com/vpfboih/ql38210/#/data/databases/SNOW_DB/schemas/SNOW_SCHEMA/stage/LOAN_AGREEMENTS_STAGE)")
                
                data = {
                        "Attribute": [
                            "Severability_Clause",
                            "Payment_Terms",
                            "Default_Clause",
                            "Interest_rate and tenure",
                            "interest rate with condition"
                        ],"Prompts": [
                            "Does document contain Severability clause?",
                            "Extract payment terms",
                            "Extract default clause",
                            "Provide interest rate and tenure at which loan was given?",
                            "Is loan interest rate greater than 9?"
                        ]
                        
                    }

                # Create the DataFrame
                df = pd.DataFrame(data)

                # Customize the dataframe size by setting width and height
                st.dataframe(df, width=700, height=250)

                job_name = st.text_input("**Enter the job name:**")

                radio_freq = ["On-Demand", "Recurring"]
                radio_selections = st.radio("**Extract Frequency**", radio_freq)

                extract_location = st.selectbox("**Extract Location**", [
                                                        "COMPANY_HOUSE_FILES_STAGE",
                                                        "MODEL_EXTRACTS_STAGE",
                                                        "FINANCIAL_DOCUMENT_STAGE",
                                                        "LOAN_AGREEMENTS_STAGE"
                                                        
                                                ])
                
                if st.button("Create Job"):
                    conn= get_snowflake_connection()
                    query = f"""
                    INSERT INTO job_table (Job_Name, Selected_Model, File_Path) VALUES ('{job_name}', '{selected_model}', '{file_path}');"""

                    # Execute the query and fetch the results
                    cur = conn.cursor()
                    cur.execute(query)
                    st.success("Job created successfully!") 

    elif menu_selection == "Review":  
        st.title('Review')
        st.markdown("""
            <style>
                div[data-testid="column"] {
                    width: fit-content !important;
                    flex: unset;
                }
                div[data-testid="column"] * {
                    width: fit-content !important;
                }
            </style>
            """, unsafe_allow_html=True)


        query = """
        Select JOB_ID, EXTRACT_TYPE, MODEL, STATUS, MODIFIED_DATE
          From JOB order by CREATED_DATE desc; """

        # Execute the query and fetch the results
        conn= get_snowflake_connection()
        cur = conn.cursor()
        cur.execute(query)
        jobs = cur.fetchall()

        # Extract only the stage names (second element in each tuple)
        
        # Convert stage_names into a DataFrame and rename the column to "Available Files"
        df_job_names = pd.DataFrame(jobs, columns=["JOB_ID", "EXTRACT_TYPE", "MODEL", "STATUS", "MODIFIED_DATE"])
        
        # If no row is selected, display the table with "View" buttons
        if st.session_state.selected_row_review is None:
            st.write('List of Submitted Jobs')
            # Display each row with buttons in columns
            for i, row in df_job_names.iterrows():
                # Create three columns for each row: two buttons and one download button
                col1, col2, col3, col4, col5, col6 = st.columns([1, 1, 1, 1, 1, 1])
                
                with col1:
                    st.write(f"**Job ID:** {row['JOB_ID']}")
                
                with col2:
                    st.write(f"**Extraction Type:** {row['EXTRACT_TYPE']}")
                
                with col3:
                    st.write(f"**Selected Model:** {row['MODEL']}")
                
                
                
                with col4:
                    st.write(f"**Status:** {row['STATUS']}")
                    
                with col5:
                    st.write(f"**Last Modified Date:** {row['MODIFIED_DATE']}")
                
                with col6:
                    if st.button('View', key=f'view_{i}'):
                        st.session_state.selected_row_review = row['JOB_ID']  # Store the selected row index

            # If session state is set, show detailed view of selected row
            st.markdown("</tbody></table>", unsafe_allow_html=True)

        # If a row is selected, display the details for that row
        else:
            pdf_files = list_pdfs(PDF_FOLDER)
            
            # Creating two main columns: one for the dropdown, one for buttons
            col1, col2 = st.columns([3, 1])

            with col1:
                selected_pdf = st.selectbox("Select a file to view", pdf_files)

            with col2:
            
                if st.button('Back'):
                        st.session_state.selected_row_review = None
                

            if selected_pdf:
                new_values_data=[]
                # Create two columns: left for the viewer, right for the key-value pairs
                col1, col2 = st.columns([2, 1])

                with col1:
                    # Display PDF preview in a scrollable box using base64 encoding
                    pdf_path = os.path.join(PDF_FOLDER, selected_pdf)
                    base64_pdf = pdf_to_base64(pdf_path)

                    # Embed the PDF using an <embed> tag and base64 data
                    pdf_display = f'<embed src="data:application/pdf;base64,{base64_pdf}" width="100%" height="800" type="application/pdf"></embed>'
                    st.markdown(pdf_display, unsafe_allow_html=True)

                with col2:
                    # Display key-value pairs for the selected file
                    
                    cursor = conn.cursor()
                    query = f"""SELECT 
                            FILE_NAME, ATTRIBUTE, VAL, NEW_VAL, NEW_VAL_REASON
                        FROM EXTRACTED_DATA
                        WHERE 
                            JOB_ID = '{st.session_state.selected_row_review}' and FILE_NAME= '{selected_pdf}'"""  
                    
                    cur.execute(query)
                    extracted_values = cur.fetchall()
                    
                    st.subheader("Extracted Values")

                    if len(extracted_values)>0:

                        for idx, item in enumerate(extracted_values):
                            file_name = item[0]
                            key = item[1]
                            original_value = item[2]
                            new_value = item[3]
                            new_value_reason = item[4]
                            
                            st.text_area(label=f"{key} (Original)", value=original_value, key=f"original_{idx}", disabled=True)

                            st.text_input(label=f"New {key}", value=new_value, key=f"new_{idx}")

                            st.text_input(label=f"Reason for changing {key}", value=new_value_reason, key=f"reason_{idx}")

                            st.write("---")  # Optional: separator line for readability
                            
                            new_values_data.append({
                                "attribute": key,
                                "extracted_value": escape_sql_value(original_value),
                                "new_value": escape_sql_value(new_value),
                                "new_value_reason": escape_sql_value(new_value_reason)
                            })
                        
                        
                        btn_submit = st.button("Submit")
                        
                        if btn_submit:
                            conn= get_snowflake_connection()

                            for idx, item in enumerate(extracted_values):
                                # Retrieve updated values from session state
                                updated_new_value = st.session_state.get(f"new_{idx}")
                                updated_new_value_reason = st.session_state.get(f"reason_{idx}")

                                # Prepare SQL Insert queries for each row
                                for entry in new_values_data:
                                    insert_query = f"""
                                    UPDATE ESG_DB.RAW.EXTRACTED_DATA
                                    SET
                                        NEW_VAL = '{updated_new_value}',           
                                        NEW_VAL_REASON = '{updated_new_value_reason}',
                                        MODIFIED_DATE = CURRENT_TIMESTAMP 
                                    WHERE
                                        JOB_ID = {st.session_state.selected_row_review}               
                                        AND FILE_NAME = '{item[0]}'    
                                        AND ATTRIBUTE = '{item[1]}'    
                                        AND FILE_NAME = '{selected_pdf}'
                                        """
                                    
                                    # Execute the query
                                    conn.cursor().execute(insert_query)

                            # Commit the transaction
                            conn.commit()

                            update_query = f"""
                                UPDATE JOB
                                SET status = 'REVIEWED'
                                WHERE JOB_ID = {st.session_state.selected_row_review}"""
                                
                            conn.cursor().execute(update_query)
                            st.success("Job is reviewed successfully!")
                    else:
                        st.write('Values are not extracted yet!')
                            



            # ---------
                
    elif menu_selection == "Analyze":  
        st.title('Analyze')

        tab_Database, tab_Files = st.tabs(["Database", "Files"])

        with tab_Database:
            yaml_files=get_yaml_file()
            if yaml_files[0]!='':
                yaml_file = st.selectbox('Please select Semantic Model', yaml_files, index=None, key="drp_semantic_model")
                if yaml_file:
                    st.session_state.yaml_file=yaml_file
                st.markdown(f"Semantic Model: `{st.session_state.yaml_file}`")

                for message_index, message in enumerate(st.session_state.messages):
                    with st.chat_message(message["role"]):
                        display_content(
                            content=message["content"],
                            request_id=message.get("request_id"),
                            message_index=message_index,
                        )

                if user_input := st.chat_input("What is your question?"):
                    if st.session_state.yaml_file:
                        process_message(prompt=user_input)
                    else:
                        st.error('Please select Semantic Model..', icon="🚨")

                if st.session_state.active_suggestion:
                    process_message(prompt=st.session_state.active_suggestion)
                    st.session_state.active_suggestion = None
            else:
                st.write('Please create semantic model to interact with database!')

        with tab_Files:
            def fetch_files_from_directory(directory_path):
                pdf_files = [f for f in os.listdir(directory_path) if f.endswith('.pdf')]
                return pdf_files


            # Function to check if embeddings exist for a file
            def check_embeddings_exist(file_name):
                embedding_path = f"embeddings/{file_name}.faiss"
                return os.path.exists(embedding_path)


            # Function to generate embeddings for documents and save them locally
            def generate_embeddings(directory_path, file_name):
                st.write("Generating embeddings for documents, please wait...")  # Progress message

                # Load the PDF file
                file_path = os.path.join(directory_path, file_name)
                loader = PyPDFLoader(file_path)
                documents = loader.load()

                # Split text into chunks
                splitter = CharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
                texts = splitter.split_documents(documents)

                # Generate embeddings
                embeddings = OpenAIEmbeddings()
                vector_store = FAISS.from_documents(texts, embeddings)

                # Save embeddings locally
                os.makedirs("embeddings", exist_ok=True)
                vector_store.save_local(f"embeddings/{file_name}.faiss")

                st.success("Embeddings generated successfully!")  # Confirmation message


            # Function to fetch document chunks based on similarity
            def fetch_document_chunks(file_name, question):
                embedding_path = f"embeddings/{file_name}.faiss"
                vector_store = FAISS.load_local(embedding_path, OpenAIEmbeddings(),
                                                allow_dangerous_deserialization=True)

                # Search for relevant chunks
                docs = vector_store.similarity_search(question, k=3)
                df_chunks = pd.DataFrame([(doc.page_content, file_name) for doc in docs],
                                         columns=["CHUNK", "RELATIVE_PATH"])
                return df_chunks


            # Function to generate a chat model prompt
            def create_prompt(question, selected_file, directory_path):
                df_context = fetch_document_chunks(selected_file, question)

                if df_context.empty:
                    st.warning("No relevant chunks found for the given question.")
                    return None, None

                prompt_context = ""
                for _, row in df_context.iterrows():
                    prompt_context += row["CHUNK"]

                prompt_context = prompt_context.replace("'", "")
                relative_path = df_context.iloc[0]["RELATIVE_PATH"]

                return f"Context: {prompt_context}\nQuestion: {question}\nAnswer: ", relative_path


            # Function to complete the question using a local model
            def complete(question, model_name, selected_file, directory_path):
                prompt, relative_path = create_prompt(question, selected_file, directory_path)

                if prompt is None:
                    return None, None

                # Placeholder for model completion - replace with actual API call if needed
                response = f"Simulated response for: {prompt}"
                return response, relative_path


            # Directory containing PDF files
            directory_path = "../data/pdf_files/ESG/"

            # Fetch files from the local directory
            if 'list_docs' not in st.session_state:
                st.session_state.list_docs = fetch_files_from_directory(directory_path)

            # Display the list of documents
            st.subheader("Chat with PDF")

            # Convert file names into a DataFrame and rename the column to "Available Files"
            df_stage_names = pd.DataFrame(st.session_state.list_docs, columns=["Available Files"])

            # Customize the dataframe size by setting width and height
            st.dataframe(df_stage_names, width=700, height=300)

            # Select a file to chat with
            st.session_state.selected_file = st.selectbox('**Select a file:**', st.session_state.list_docs)

            # Select a model (modify if you have models locally or API access)
            model = st.selectbox('**Select your model:**', [
                'mixtral-8x7b', 'snowflake-arctic', 'mistral-large',
                'llama3-8b', 'llama3-70b', 'reka-flash',
                'mistral-7b', 'llama2-70b-chat', 'gemma-7b'
            ], key="model_selection")

            # Input for user's question
            question = st.text_input("**Enter your question**", placeholder="Ask a question about your document...")

            # Add submit button
            submit = st.button("Submit")

            if submit and question and st.session_state.selected_file:
                # Check if embeddings already exist for the file
                if not check_embeddings_exist(st.session_state.selected_file):
                    with st.spinner('Generating embeddings...'):
                        generate_embeddings(directory_path, st.session_state.selected_file)
                else:
                    st.write("Embeddings already exist for this file. Proceeding with the question.")

                # Process the question and generate a response
                with st.spinner('Processing your question...'):
                    response, relative_path = complete(question, model, st.session_state.selected_file, directory_path)

                if response:
                    st.write("**Answer:**")
                    st.markdown(response)
                    st.write("")
                    st.write("**Chunks of Similar Documents:**")
                    df_chunks = fetch_document_chunks(st.session_state.selected_file, question)

                    for index, row in df_chunks.iterrows():
                        st.write(f"**Document:** {row['RELATIVE_PATH']}")
                        st.write(row)
                        st.write("---")

                else:
                    st.warning("No response generated. Please try a different question.")



    elif menu_selection == "Data Catalogue":
        st.title('Data Catalogue')

        tab_Semantic, tab_Internal_Metadata, tab_All_Metadata = st.tabs(["Semantic Models", "Internal Metadata", "Available Metadata"])

        with tab_Semantic:

            col1, col2 = st.columns([4,1])
            with col1:
                pass
            with col2:
                if st.button(f'Create New Model', key=f'bt_view_100'):
                    builder.show()

            if "page" not in st.session_state:
                st.session_state["page"] = GeneratorAppScreen.ONBOARDING

            if st.session_state["page"] == GeneratorAppScreen.ITERATION:
                iteration.show()


            # If no row is selected, display the table with "View" buttons
            if st.session_state.selected_row_dc_sm_model is None:
                yaml_files=get_yaml_file()
                if yaml_files[0]!= "":
                    st.write("**List of Semantic Models:**")
                    for i in range(len(yaml_files)):
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write(f"**Name:** {yaml_files[i]}")
                        with col2:
                            if st.button(f'View', key=f'bt_view_{i}'):
                                st.session_state.selected_row_dc_sm_model = i  # Store the selected row index

            else:
                #selected_row_data = df_stage_names.iloc[st.session_state.selected_row_dc_intenal]
                file_name=st.session_state.selected_row_dc_sm_model
                st.write("### Detailed View")
                st.write(f"**File Name:** {file_name}")
                iteration.show()

                if st.button('Back'):
                    st.session_state.selected_row_dc_sm_model = None

        with tab_Internal_Metadata:

            file_path = "../data/data_catalog_metadatafile/metadata.xlsx"

            # Read the Excel file
            metadata_df = pd.read_excel(file_path)

            # Add some CSS to make the table look better
            st.markdown("""
                <style>
                    /* Style the table */
                    table {
                        border-collapse: collapse;
                        width: 100%;
                        font-size: 14px;
                    }
                    th {
                        background-color: #4CAF50;
                        color: white;
                        font-weight: bold;
                        text-align: left;
                        padding: 8px;
                    }
                    td {
                        padding: 8px;
                        text-align: left;
                    }
                    tr:nth-child(even) {
                        background-color: #f2f2f2;
                    }
                    tr:hover {
                        background-color: #ddd;
                    }
                </style>
            """, unsafe_allow_html=True)

            # Display the data using Streamlit's dataframe function with wider columns
            st.dataframe(metadata_df, use_container_width=True)

        with tab_All_Metadata:

            # Load environment variables
            load_dotenv()
            os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")

            # Define Snowflake connection parameters
            snowflake_account = 'APVLSLK-BG69069'
            snowflake_user = 'Harshita'
            snowflake_password = 'Harshita123'
            snowflake_database = 'META_DB'
            snowflake_schema = 'METADATA_SCHEMA'

            # Streamlit app setup
            st.title("Snowflake Metadata Explorer")
            st.write("This application allows you to query the Snowflake database metadata.")

            # User input for the question
            user_question = st.text_input("Enter your question about the database metadata:")

            # Add a submit button
            submit_button = st.button("Submit")


            # Function to load metadata from Snowflake
            def load_and_process_metadata():
                # Connect to Snowflake
                conn = connector.connect(
                    user=snowflake_user,
                    password=snowflake_password,
                    account=snowflake_account,
                    warehouse='COMPUTE_WH',  # Replace with your warehouse
                    database=snowflake_database,
                    schema=snowflake_schema
                )

                # Query to fetch metadata from the METADATA_TABLE
                query = "SELECT DB_NAME, SCHEMA_NAME, TABLE_NAME, METADATA FROM META_DB.METADATA_SCHEMA.METADATA_TABLE"

                # Fetch metadata
                metadata_df = pd.read_sql(query, conn)

                # Close the connection
                conn.close()

                # Create a list to hold documents
                documents = []

                for index, row in metadata_df.iterrows():
                    documents.append(
                        f"Database: {row['DB_NAME']}, Schema: {row['SCHEMA_NAME']}, Table: {row['TABLE_NAME']}, Metadata: {row['METADATA']}")

                # Split the text into smaller chunks
                text_splitter = CharacterTextSplitter(chunk_size=1000, chunk_overlap=0)
                docs = text_splitter.create_documents(documents)

                return docs


            # Function to initialize FAISS and embeddings
            def create_faiss_vectorstore(docs):
                embeddings = OpenAIEmbeddings()
                vectorstore = FAISS.from_documents(docs, embeddings)
                return vectorstore


            # Function to create a custom prompt template
            def create_prompt_template():
                prompt_template = """
                    You are an intelligent database assistant. Your task is to accurately verify if specific information exists in the database.
                    Always validate the question against the database accurately, and do not make assumptions or provide incorrect information.
                    If the information related to '{question}' is available in the database, 
                    respond only with: 'The information is present in database name and within table name.'
                    Do not provide any information about columns or data in the table; the user must raise a request for such access.
                    If the information related to '{question}' is not present in the database after verifying it carefully,
                    respond only with: 'The information is not present in the database.'
                    Given the following context: {context}
                """
                return PromptTemplate(
                    input_variables=["context", "query"],
                    template=prompt_template
                )


            # Function to process the user's query
            def process_query(question):
                # Load and process the metadata from Snowflake
                docs = load_and_process_metadata()

                # Create FAISS vector store
                vectorstore = create_faiss_vectorstore(docs)

                # Initialize the language model (LLM) with a prompt template
                llm = ChatOpenAI(model="gpt-4o")

                # Create a retriever using FAISS
                retriever = vectorstore.as_retriever()

                # Define a RetrievalQA chain
                qa_chain = RetrievalQA.from_chain_type(
                    llm=llm,
                    chain_type="stuff",
                    retriever=retriever,
                    return_source_documents=False,
                    chain_type_kwargs={"prompt": create_prompt_template()}
                )

                # Pass the user question as the 'query' key
                answer = qa_chain.run({
                    "query": question,
                    "context": "",  # Initially no context; FAISS will retrieve it
                })
                return answer


            # If the submit button is clicked, process and display the response
            if submit_button and user_question:
                st.write("Processing your question...")
                answer = process_query(user_question)
                st.write(f"**Question:** {user_question}")
                st.write(f"**Answer:** {answer}")
