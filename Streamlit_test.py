import os
import configparser
from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
import pandas as pd
import streamlit as st

st.set_page_config(
    layout="wide",
    page_title="Streamlit Convention"
)

st.title('Streamlit Convention')

config = configparser.ConfigParser()
config.read('config.ini')
sections = config.sections()
accounts = []
for section in sections:
    accounts.append(section)

def sfAccount_selector(account):
    #setup config.ini read rules
    sfAccount = config[account]['sfAccount']
    sfUser = config[account]['sfUser']
    sfPass = config[account]['sfPass']
    sfRole = config[account]['sfRole']
    sfDB = config[account]['sfDB']
    sfSchema = config[account]['sfSchema']
    sfWarehouse = config[account]['sfWarehouse']

    #dictionary with names and values of connection parameters
    conn = {"driver": "snowflake",
            "account": sfAccount,
            "user": sfUser,
            "password": sfPass,
            "role": sfRole,
            "warehouse": sfWarehouse,
            "database": sfDB,
            "schema": sfSchema}
    return conn

def session_builder(conn):
    session = Session.builder.configs(conn).create()
    return session

def db_list(session):
    dbs = session.sql("show databases ;").collect()
    #db_list = dbs.filter(col('name') != 'SNOWFLAKE')
    db_list = [list(row.asDict().values())[1] for row in dbs]
    return db_list


def schemas_list(chosen_db, session):
    # .table() tells us which table we want to select
    # col() refers to a column
    # .select() allows us to chose which column(s) we want
    # .filter() allows us to filter on coniditions
    # .distinct() means removing duplicates
    
    session.sql('use database :chosen_db;')
    fq_schema_name = chosen_db+'.information_schema.tables'
    

    schemas = session.table(fq_schema_name)\
            .select(col("table_schema"),col("table_catalog"),col("table_type"))\
            .filter(col('table_schema') != 'INFORMATION_SCHEMA')\
            .filter(col('table_type') == 'BASE TABLE')\
            .distinct()
            
    schemas_list = schemas.collect()
    # The above function returns a list of row objects
    # The below turns iterates over the list of rows
    # and converts each row into a dict, then a list, and extracts
    # the first value
    schemas_list = [list(row.asDict().values())[0] for row in schemas_list]
    return schemas_list

def tables_list(chosen_db, chosen_schema, session):

    fq_schema_name = chosen_db+'.information_schema.tables'
    #tables = session.table('sf_demo.information_schema.tables')\
    tables = session.table(fq_schema_name)\
        .select(col('table_name'), col('table_schema'), col('table_type') )\
        .filter(col('table_schema') == chosen_schema)\
        .filter(col('table_type') == 'BASE TABLE')\
        .sort('table_name')
    tables_list = tables.collect()
    tables_list = [list(row.asDict().values())[0] for row in tables_list]
    return tables_list

def table_choice(session, value, index):
    st.write('Data for {} Table'.format(value))
    database = db_list(session)
    db_select = st.selectbox('Choose {} Database'.format(value),(database), index=index)
    conn["database"] = db_select
    schemas = schemas_list(db_select, session)
    sc_select = st.selectbox('Choose {} Schema'.format(value),(schemas))
    conn["schema"] = sc_select
    tables = tables_list(db_select,sc_select, session)
    tb_select = st.selectbox('Choose {} table'.format(value),(tables))
    conn["table"] = tb_select
    snowflake_table = session.sql('select * from {}.{}.{};'.format(db_select,sc_select,tb_select)).toPandas()
    columns = session.sql('desc table {}.{}.{};'.format(db_select,sc_select,tb_select)).collect()
    columns = [list(row.asDict().values())[0] for row in columns]
    columns_disp = st.multiselect('Select any 2 columns to display',columns)
    return {'snowflake_table':snowflake_table, 'database':db_select, 'schema': sc_select, 'table':tb_select, 'columns':columns_disp}



with st.sidebar:
    acc_select = st.selectbox('Choose account',(accounts))
    conn = sfAccount_selector(acc_select)
    session = session_builder(conn)
    table = table_choice(session, 'a', 0)

if len(table['columns']) == 2:
    st.line_chart(table['snowflake_table'],x = table['columns'][0],y = table['columns'][1])

if len(table['columns']) > 2:
    st.markdown('You have selected more than 2 columns')

