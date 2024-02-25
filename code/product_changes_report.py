
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

def select_query_pfv(date1):

    query = f"""
    select DATE(created_at) as created_at, max(created_at) as max, 
    product_type, product_id, 
    GROUP_CONCAT(DISTINCT field_name SEPARATOR', '), 
    scheduled_at 
    from pending_field_values
    where created_at >= '{date1}'
    and product_type <> 'MppTab'
    GROUP BY DATE(created_at), product_type, product_id
    """
    
    ### TBC
    # Check that manually entered changes really do all appear here
    # now add provider and product NAMES
    # join to product_group, and group on that?
    # check logic about scheduled changes... alert when entered or when executed or both? Entered works, right?

    return query


def select_query_gts(date1):
   
    query = f"""
    select product_type, product_id
    from gts_link_versions
    where created_at >= '{date1}'
    and active = 1
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



# %%
# connections 
#sqlEngine = sqlEngineCreator('aircamel_rep_username', 'aircamel_rep_password', 'aircamel_rep_host', 'aircamel_rep_db')
sqlEngine_ethercat = sqlEngineCreator('ethercat_username', 'ethercat_password', 'ethercat_host', 'ethercat_db')
sqlEngine_spacecoyote = sqlEngineCreator('spacecoyote_username', 'spacecoyote_password', 'spacecoyote_host', 'spacecoyote_admin_db')

gs_auth = pygsheets.authorize(service_file='../auth/mozo-private-dev-19de22e18578.json')

gsheets = config.cred_info['gsheets']
tab_string = 'Sheet1'


#%%
# set date ranges

from datetime import date, datetime, timedelta

today_date = date.today()
today_string = today_date.strftime("%Y-%m-%d")


### TBC 
### look up the most recent date written to gsheets and then remove this test
### safest method would be to record the latest datestamp in the success.txt

pfv_date = '2024-02-23'


# recent gts date
gts_date = (datetime.strptime(pfv_date, "%Y-%m-%d") - timedelta(days=40)).strftime("%Y-%m-%d")



#%%
# execute

error_flag = False
try:
    for gs in gsheets:
        # test   gs = gsheets[0]

        key = gs['gsheets_key']
        sheets_file = gs_auth.open_by_key(key)

        # EXTRACT 1
        with sqlEngine_spacecoyote.connect() as dbConnection:
            query = select_query_pfv(pfv_date)
            data_pfv = pd.read_sql(sql=query, con=dbConnection)    

        # EXTRACT 2
        with sqlEngine_ethercat.connect() as dbConnection:
            query = select_query_gts(gts_date)
            data_gts = pd.read_sql(sql=query, con=dbConnection)  

        # TRANSFORM 1
        # no NaNs
        data_pfv = data_pfv.fillna(value=0)
        # timstamps into strings
        for c in ['created_at', 'scheduled_at', 'max']:
            data_pfv[c] = [str(t) for t in data_pfv[c]]


        # JOIN

        ### TBC the join
        
 
        # tidy ups
        #result = result.reset_index(drop=True)
        
        # LOAD
        write_to_gsheet(sheets_file, tab_string, result)
 
except:
    error_flag = True


# %%
# success/error files to drive email, and latest success date file to ensure no gaps  
    
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
    ### include code to save the last date successfully written to gsheets
    f = open(data_proc_path + "success_date.txt", "a")
    f.write('TBC')
    f.close()




# %%
