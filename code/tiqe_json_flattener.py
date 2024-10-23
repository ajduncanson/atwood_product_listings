
"""TIQE

Queries our AWS DBs and flattens json objects for use in google sheets.

Run this file from within the /code dir of the project

"""
#%% 
# setup

import os
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import pytz
import pickle
from sqlalchemy import create_engine
import pygsheets
import json
from pandas import json_normalize

import atwood_product_listings_report_config as config

tz = pytz.timezone('Australia/Sydney')
filesavetime = datetime.now(tz).strftime("%Y%m%d_%H%M")
today_date = date.today()
pageview_date = (today_date - timedelta(days=7)).strftime("%Y-%m-%d")

# file save details
curpath = os.path.abspath(os.curdir)
data_raw_path = curpath + '/../data/raw/'
data_proc_path = curpath + '/../data/tiqe/'

if os.path.exists(data_proc_path + "success.txt"):
    os.remove(data_proc_path + "success.txt")
if os.path.exists(data_proc_path + "error.txt"):
    os.remove(data_proc_path + "error.txt")


#%%
# functions

def sqlEngineCreator(user, pword, host, db):

    username = config.cred_info[user]
    password = config.cred_info[pword]
    host = config.cred_info[host]
    db = config.cred_info[db]
    sqlEngine = create_engine(f'mysql+pymysql://{username}:{password}@{host}/{db}') #, pool_size=20, max_overflow=0)
    return sqlEngine

#%% 
# connections
gsheet_key = config.cred_info['gsheet_key']

#%%
# define DB queries


def select_query(i):
    query = f"""
    select request_url, third_party_response
    from 
    `ferris_staging_3`.`travel_insurance_quote_engine_response_comparisons` 

    where id = {i}
    """
    return query


#%%
# # get DB data


# error_flag = False

# # connection to current db list of endpoints
# sqlEngine = sqlEngineCreator('waterfox_username', 'waterfox_password', 'waterfox_host', 'waterfox_3_db')

# try:
#         with sqlEngine.connect() as dbConnection:
#             query = select_query(27)
#             db_results = pd.read_sql(sql=query, con=dbConnection)
#             result = db_results

# except:
#     error_flag = True

#%% 
# flatten json

#### handmade code for sample of SCTI response
with sqlEngine.connect() as dbConnection:
    query = select_query(27)
    db_results = pd.read_sql(sql=query, con=dbConnection)
    result = db_results

# convert string to json
response = [json.loads(e) for e in result['third_party_response']]

#flatten top level
flat = json_normalize(response[0])
#flatten benefits level
premiums = [json_normalize(q) for q in flat['premiums']]

#%%
#### handmade code for sample of Fastcover response

with sqlEngine.connect() as dbConnection:
    query = select_query(28)
    db_results = pd.read_sql(sql=query, con=dbConnection)
    result = db_results

# convert string to json
response = [json.loads(e) for e in result['third_party_response']]

#flatten top level
flat = json_normalize(response)
#flatten benefits level
quotes = [json_normalize(q) for q in flat['quotes']]
very_flat = json_normalize(flat['quotes'][0])

#%%
#### handmade code for sample of TravelInsuranceSaver response

with sqlEngine.connect() as dbConnection:
    query = select_query(29)
    db_results = pd.read_sql(sql=query, con=dbConnection)
    result = db_results

# convert string to json
response = [json.loads(e) for e in result['third_party_response']]

#flatten top level
flat = json_normalize(response[0]['Envelope']['Body']['policyResponse']['products']['product'])
#flatten benefits level
benefits = [json_normalize(q) for q in flat['benefits.benefit']]
benefits = [k['name'] for k in flat['benefits.benefit'][0]]


#%%
# now make a df (result of the above is a list with only 1 element [0])
quotes_df = pd.DataFrame(quotes[0])




# %%
#save to csv

quotes_df.to_csv(data_proc_path + 'test_flat_json_' + filesavetime + '.csv')

#%%

