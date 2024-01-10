
"""daily click reports

get click stats and populate gsheets

Run this file from within the /code dir of the project

"""
#%% 
# setup

import os
import pandas as pd
import numpy as np
import datetime
import pytz
import pickle
from sqlalchemy import create_engine
import pygsheets

import daily_click_reports_config as config

time_zone = pytz.timezone('Australia/Sydney')
filesavetime = datetime.datetime.now().strftime("%Y%m%d_%H%M")

# file save details
curpath = os.path.abspath(os.curdir)
data_proc_path = curpath + '/../data/daily_click_reports/'
fname = 'daily_click_reports'

if os.path.exists(data_proc_path + "success.txt"):
    os.remove(data_proc_path + "success.txt")
if os.path.exists(data_proc_path + "error.txt"):
    os.remove(data_proc_path + "error.txt")

#%%
# functions to select from sql and write to gsheets

def sqlEngineCreator(user, pword, host, db):

    username = config.cred_info[user]
    password = config.cred_info[pword]
    host = config.cred_info[host]
    db = config.cred_info[db]
    sqlEngine = create_engine(f'mysql+pymysql://{username}:{password}@{host}/{db}') #, pool_size=20, max_overflow=0)
    return sqlEngine

def select_query(provider_id, product_type, test_date):
    query = f"""
    select `date`, count(*), sum(earnings_per_e2e)
    from rcd
	where `date` = '{test_date}'  
	  and source = 'gts'
	  and provider_id = {provider_id} and product_type = '{product_type}'
	  group by `date`	  
	  order by `date`
	  ;
    """
    return query

def write_to_gsheet(key, tab_title, data):
    try:
        sh = gc.open_by_key(key)
        # note that the Sheets file must be shared with pygsheets@mozo-private-dev.iam.gserviceaccount.com

        wks = sh.worksheet_by_title(tab_title)
        wks.set_dataframe(data,(1,1))

    except:
        error_flag = True

# %%
# connections 
sqlEngine = sqlEngineCreator('aircamel_rep_username', 'aircamel_rep_password', 'aircamel_rep_host', 'aircamel_rep_db')
gc = pygsheets.authorize(service_file='../auth/mozo-private-dev-19de22e18578.json')

#%%
# execute

result = dict()
error_flag = False

try:
    for gs in config.gsheets:

        # test    prod = 'HomeLoan'
        prod = gs['product_type']
        prov = gs['provider']
        key = gs['gsheets_key']

        with sqlEngine.connect() as dbConnection:
            query = select_query(prov,prod)
            db_results = pd.read_sql(sql=query, con=dbConnection)    

        write_to_gsheet(key, 'Sheet1', db_results)
        result[prod] = db_results

except:
    error_flag = True

# %%
# save pickle

pname = data_proc_path + fname + '_' + filesavetime + '.pkl'
with open(pname, 'wb') as file: 
    pickle.dump(result, file) 

# %%
# error flag file to drive shell script email reporting
    
if error_flag:
    if os.path.exists(data_proc_path + "success.txt"):
        os.remove(data_proc_path + "success.txt")
    f = open(data_proc_path + "error.txt", "a")
    f.write("error " + filesavetime)
    f.close()
else:
    if os.path.exists(data_proc_path + "error.txt"):
        os.remove(data_proc_path + "error.txt")
    f = open(data_proc_path + "success.txt", "a")
    f.write("success " + filesavetime)
    f.close()

# %%
