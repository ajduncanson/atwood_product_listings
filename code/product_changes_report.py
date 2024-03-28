
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
import requests

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

def get_pfv_grouped(date1):

    query = f"""
    select DATE(created_at) as created_at, max(created_at) as max, 
    product_type, product_id, 
    GROUP_CONCAT(DISTINCT field_name SEPARATOR', ') as changes, 
    scheduled_at
    from pending_field_values
    where created_at >= '{date1}'
    and product_type <> 'MppTab'
    GROUP BY DATE(created_at), product_type, product_id
    """
    
    with sqlEngine_spacecoyote.connect() as dbConnection:
        result = pd.read_sql(sql=query, con=dbConnection)

    ### TBC
    # Check that manually entered changes really do all appear here

    return result

def get_pfv(date1):

    query = f"""
    select DATE(created_at) as created_at,  
    product_type, product_id, 
    field_name as changes, 
    '' as previous_value,
    CASE WHEN (product_type = 'TermDeposit' and field_name = 'interest_rate_tiers') THEN 'ask Research Team for the new interest rate'
    ELSE REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(field_value, '(---( )?)', ''),'\\n','; '),''\';', '') END as new_value,
    scheduled_at, created_at as added_to_admin,
    'mozo.com.au' as product_page_link
    from pending_field_values
    where created_at >= '{date1}'
    and product_type <> 'MppTab'
    and field_name <> 'gts'
    """
    
    with sqlEngine_spacecoyote.connect() as dbConnection:
        result = pd.read_sql(sql=query, con=dbConnection)

    ### TBC
    # Check that manually entered changes really do all appear here

    return result


def get_gts(date1):
   
    query = f"""
    select product_type, product_id
    from gts_link_versions
    where created_at >= '{date1}'
    and active = 1
    """
    with sqlEngine_ethercat.connect() as dbConnection:
        result = pd.read_sql(sql=query, con=dbConnection)  

    return result

def get_product_name_ids(dict):

    all_products = pd.DataFrame()

    for k,v in dict.items():
        query = f"""
        select '{k}' as product_type, 
        a.provider_id, a.id as product_id, a.product_group_id,
        p.name as provider_name, a.name as product_name
        from {table_name(k, version = False)} a
        left join providers p
        on a.provider_id = p.id
        where a.id in {'(' + ','.join([str(e) for e in v]) + ')'}
        """
        with sqlEngine_ethercat.connect() as dbConnection:
            this_one = pd.read_sql(sql=query, con=dbConnection) 
        all_products = pd.concat([all_products, this_one])

    return all_products

def get_provider_names():

    query = f"""
    select id, name as provider_name
    from providers
    """
    with sqlEngine_ethercat.connect() as dbConnection:
        providers = pd.read_sql(sql=query, con=dbConnection) 

    return providers

def write_to_gsheet(sh, tab_title, data):
    
# test      sh = sheets_file; tab_title = 'Sheet2'; data = result
  
    # find worksheet and add new data to it
    # for append_table, the data needs to be a list of lists; the inner lists are the rows to add
    try:
        data = data.values.tolist()
        wks = sh.worksheet_by_title(tab_title)
        wks.append_table(data, start = 'A2', dimension = 'ROWS', overwrite = False)
    except:
        error_flag = True



# %%
# connections 
#sqlEngine = sqlEngineCreator('aircamel_rep_username', 'aircamel_rep_password', 'aircamel_rep_host', 'aircamel_rep_db')
sqlEngine_ethercat = sqlEngineCreator('ethercat_username', 'ethercat_password', 'ethercat_host', 'ethercat_db')
sqlEngine_spacecoyote = sqlEngineCreator('spacecoyote_username', 'spacecoyote_password', 'spacecoyote_host', 'spacecoyote_admin_db')

gs_auth = pygsheets.authorize(service_file='../auth/mozo-private-dev-19de22e18578.json')

gsheets = config.cred_info['gsheets']
tab_string = 'Sheet2'


#%%
# set date ranges

from datetime import date, datetime, timedelta

today_date = date.today()
today_string = today_date.strftime("%Y-%m-%d")

run_timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")

### TBC 
### look up the most recent date written to gsheets and then remove this test
### safest method would be to record the latest datestamp in the success.txt

pfv_date = '2024-03-02'


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
        data_pfv = get_pfv(pfv_date)

        # EXTRACT 2
        data_gts = get_gts(gts_date)

        # LOOP EXTRACT PRODUCT & PROVIDER NAMES
        product_dict = data_gts.groupby('product_type')['product_id'].apply(list).to_dict()
        monetised_products = get_product_name_ids(product_dict)


        # TRANSFORM
        # no NaNs
        data_pfv = data_pfv.fillna(value=0)
        data_pfv['run_timestamp'] = run_timestamp
        # timstamps into strings
        for c in ['created_at']:
            data_pfv[c] = [str(t) for t in data_pfv[c]]
        for c in ['scheduled_at', 'added_to_admin']:
            data_pfv[c] = [t.strftime("%Y-%m-%d_%H%M") if t != 0 else '' for t in data_pfv[c]]
         


        # JOIN
        result = pd.merge(left = data_pfv, right = monetised_products, how = 'inner', on = ['product_type', 'product_id'])


        ### TBC ###

        # atwood = read csv /data/atwood_products/atwood_products_latest.csv
        # get rid of atwood recency 1 and leave recency 3 (<180 days since last update)
        # inner join result and atwood, on product type and product id, leaving only the changes on monetised pages on atwood

        # copy this and select the columns needed for product changes and write to gsheets
        # second copy selecting only unique product type, id and page; wriet the to the other tab of the gsheet and it becaomse the action list

 
        # tidy ups
        result = result.reset_index(drop=True)
        result = result.sort_values(by = ['created_at', 'product_type', 'provider_name', 'product_name', 'changes', 'added_to_admin'], axis = 0)
        col_order = ['created_at', 'product_type', 
                     'provider_name', 'product_name',
                     'changes', 'previous_value', 'new_value', 
                     'scheduled_at', 'added_to_admin',
                     'run_timestamp', 
                     'provider_id', 'product_group_id', 'product_id', 'product_page_link']
        result = result[col_order]

        
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
    # send to webhook to trigger slack message
    slack_webhook = 'https://hooks.slack.com/triggers/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'
    body = '{"timestamp": "' + run_timestamp + '"}'
    r = requests.post(url=slack_webhook, data=body)


# %%
