import streamlit as st
import pandas as pd
import snowflake.connector
from datetime import datetime
import json
import re
import os

# Folder where PDFs are stored
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Get the directory of the current script
PDF_FOLDER = os.path.join(BASE_DIR, "pdf_files/SFP")       # Combine with the 'pdf_files' folder

# Function to check if embeddings exist for a file
def check_embeddings_exist(conn, file_name):
    query = "SELECT COUNT(*) AS COUNT FROM DOCS_CHUNKS_TABLE WHERE RELATIVE_PATH = %s"
    cur = conn.cursor()
    try:
        cur.execute(query, (file_name,))
        result = cur.fetchone()
        return result[0] > 0
    finally:
        cur.close()

# Function to generate embeddings for documents
def generate_embeddings(conn):
    st.write("Generating embeddings for documents, please wait...")  # Progress message
    
    query = f"""
    INSERT INTO DOCS_CHUNKS_TABLE (relative_path, size, file_url, scoped_file_url, chunk, chunk_vec)
    SELECT relative_path, 
        size,
        file_url, 
        build_scoped_file_url(@loan_agreements_stage, relative_path) AS scoped_file_url,
        func.chunk AS chunk,
        SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', func.chunk) AS chunk_vec
    FROM 
        directory(@loan_agreements_stage) AS dir,
        TABLE(pdf_text_chunker(build_scoped_file_url(@loan_agreements_stage, relative_path))) AS func
    """
    cur = conn.cursor()
    try:
        cur.execute(query)
        conn.commit()
        st.success("Embeddings generated successfully!")  # Confirmation message
    except Exception as e:
        st.error(f"An error occurred while generating embeddings: {e}")
    finally:
        cur.close()

# Function to fetch files from Snowflake stage
def fetch_files(conn, selected_stage):
    return os.listdir(PDF_FOLDER)

# Function to fetch document chunks
def fetch_document_chunks(conn, file_name, question):
    num_chunks = 3
    query = """
    WITH results AS (
        SELECT dc.RELATIVE_PATH, dc.CHUNK
        FROM DOCS_CHUNKS_TABLE dc
        WHERE dc.RELATIVE_PATH = %s
        ORDER BY VECTOR_COSINE_SIMILARITY(dc.chunk_vec, SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', %s)) DESC
        LIMIT %s
    )
    SELECT CHUNK, RELATIVE_PATH FROM results
    """
    cur = conn.cursor()
    try:
        cur.execute(query, (file_name, question, num_chunks))
        df_chunks = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
        return df_chunks
    finally:
        cur.close()

# Function to generate a chat model prompt
def create_prompt(conn, question, selected_file, selected_stage):
    df_context = fetch_document_chunks(conn, selected_file, question)
    
    if df_context.empty:
        st.warning("No relevant chunks found for the given question.")
        return None, None, None

    prompt_context = ""
    for _, row in df_context.iterrows():
        prompt_context += row["CHUNK"]

    prompt_context = prompt_context.replace("'", "")
    relative_path = df_context.iloc[0]["RELATIVE_PATH"]

    query = f"SELECT GET_PRESIGNED_URL(@{selected_stage}, %s, 360) AS URL_LINK"
    cur = conn.cursor()
    try:
        cur.execute(query, (relative_path,))
        url_link = cur.fetchone()[0]
        return f"Context: {prompt_context}\nQuestion: {question}\nAnswer: ", url_link, relative_path
    finally:
        cur.close()

# Function to complete the question using Snowflake Cortex
def complete(conn, myquestion, model_name, selected_file, selected_stage):
    prompt, url_link, relative_path = create_prompt(conn, myquestion, selected_file, selected_stage)
    
    if prompt is None:
        return None, None, None

    # Execute the query to get the model completion response
    cmd = "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s,%s) AS RESPONSE"
    cur = conn.cursor()
    try:
        cur.execute(cmd, (model_name, prompt))
        df_response = cur.fetchone()
        return df_response[0], url_link, relative_path
    finally:
        cur.close()
