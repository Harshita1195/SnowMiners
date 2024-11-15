import streamlit as st
import os
import pandas as pd
from snowflake import connector
from langchain.text_splitter import CharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
# from langchain_google_genai import ChatGoogleGenerativeAI, GoogleGenerativeAIEmbeddings
from langchain.vectorstores import FAISS
from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

# Load environment variables
load_dotenv()
os.environ["GOOGLE_API_KEY"] = os.getenv("GOOGLE_API_KEY")

# Define Snowflake connection parameters
snowflake_account = account
snowflake_user = user
snowflake_password = password
snowflake_database = 'META_DB'
snowflake_schema = 'DATADICTIONARY'

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
    
    # Query to fetch metadata from the DatabaseMetadata table
    query = "SELECT DATABASE, SCHEMAS, TABLES, METADATA FROM DatabaseMetadata"
    
    # Fetch metadata
    metadata_df = pd.read_sql(query, conn)
    
    # Close the connection
    conn.close()
    
    # Create a list to hold documents
    documents = []
    
    for index, row in metadata_df.iterrows():
        documents.append(f"Database: {row['DATABASE']}, Schemas: {row['SCHEMAS']}, Tables: {row['TABLES']}, Metadata: {row['METADATA']}")

    # Split the text into smaller chunks
    text_splitter = CharacterTextSplitter(chunk_size=1000, chunk_overlap=0)
    docs = text_splitter.create_documents(documents)
    
    return docs

# Function to initialize FAISS and embeddings
def create_faiss_vectorstore(docs):
    # embeddings = GoogleGenerativeAIEmbeddings(model="models/embedding-001")
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
    # llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash")
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
