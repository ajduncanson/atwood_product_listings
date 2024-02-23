
"""product changes report

get products that have had changes made or scheduled and populate gsheets

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

import product_changes_report_config as config

time_zone = pytz.timezone('Australia/Sydney')
filesavetime = datetime.datetime.now().strftime("%Y%m%d_%H%M")

# file save details
curpath = os.path.abspath(os.curdir)
data_proc_path = curpath + '/../data/product_changes_report/'
fname = 'product_changes_report'

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

def camelToSnake(str):
     
    return ''.join(['_'+i.lower() if i.isupper() 
               else i for i in str]).lstrip('_')
     
def table_name(productType, version = False):

    if version == True:
        suffix = '_versions'
    else:
        suffix = 's'
    return camelToSnake(productType) + suffix

def select_query(prod, dates):


#### TBC: this is where the query needs to go, to 
    # new version records compared with old ones, or perhaps a product table diff
    # union with pending field values
    # join with list of gts_link_versions from the past x days


    query = f"""
    select id, updated_at
    from {table_name(prod, version = True)}
	where {dates}  
	;
    """
    return query


def write_to_gsheet(sh, tab_title, data):
    
# test      sh = sheets_file; tab_title = tab; data = result
  
    # find worksheet and add new data to it
    # for append_table, the data needs to be a list of lists; the inner lists are the rows to add
    try:
        data = data.values.tolist()
        wks = sh.worksheet_by_title(tab_title)
        wks.append_table(data, start = 'A1', dimension = 'ROWS', overwrite = False)
    except:
        error_flag = True


#%%

def extract_transform_load(date_string, sheets_file, tab):

    #test   date_string = month_string; tab = tab_string

    # EXTRACT
    with sqlEngine.connect() as dbConnection:
        query = select_query('HomeLoan', date_string)
        db_results = pd.read_sql(sql=query, con=dbConnection)    

    # TRANSFORM
    # no NaNs
    result = db_results.fillna(value=0)
    # timstamps into strings
    result['updated_at'] = [str(t) for t in result['updated_at']]
    # tidy ups
    #result = result.reset_index(drop=True)
    
    # LOAD
    write_to_gsheet(sheets_file, tab, result)

    return result

# %%
# connections 
sqlEngine = sqlEngineCreator('aircamel_rep_username', 'aircamel_rep_password', 'aircamel_rep_host', 'aircamel_rep_db')

sqlEngine = sqlEngineCreator('ethercat_username', 'ethercat_password', 'ethercat_host', 'ethercat_db')




gs_auth = pygsheets.authorize(service_file='../auth/mozo-private-dev-19de22e18578.json')

gsheets = config.cred_info['gsheets']

#%%
# set date ranges

from datetime import date, timedelta

today_date = datetime.date.today()
yesterday_date = today_date - timedelta(days=1)
this_first = today_date.replace(day=1)

today_string = today_date.strftime("%Y-%m-%d")
this_first_string = this_first.strftime("%Y-%m-%d")

month_string = '`updated_at` >= "' + this_first_string + '" and `updated_at` < "' + today_string + '"'

month_range = pd.date_range(start=this_first, end=yesterday_date).date

tab_string = 'Sheet1'


#%%
# execute

error_flag = False
try:
    for gs in gsheets:
        # test   gs = gsheets[0]

        key = gs['gsheets_key']
        sheets_file = gs_auth.open_by_key(key)

        extract_transform_load(month_string, sheets_file, tab_string)   
 
except:
    error_flag = True


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
