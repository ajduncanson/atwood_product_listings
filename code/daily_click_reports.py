
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

def select_query(provider, product_type, dates):
    query = f"""
    select `date`, count(*) as Clicks, sum(earnings_per_e2e) as Spend
    from rcd
	where {dates}  
	  and source = 'gts'
	  and provider_name = '{provider}' and product_type = '{product_type}'
	  group by `date`	  
	  order by `date`
	  ;
    """
    return query

def write_to_gsheet(sh, tab_title, data):
    
    # get the right tab, or make one
    try:
        # get tab
        wks = sh.worksheet_by_title(tab_title)
    except:
        # make tab
        try:
            sh.add_worksheet(tab_title, rows=100, cols=10, index = 0)
            wks = sh.worksheet_by_title(tab_title)
            wks.apply_format(ranges=['B2:B35'], format_info={'numberFormat': {'type': 'NUMBER'}})
            wks.apply_format(ranges=['C2:C35'], format_info={'numberFormat': {'type': 'CURRENCY'}})
            
            #apply_format('C1:C100', {'numberFormat': {'type': 'CURRENCYs'}}) 

        except:
            error_flag = True
    
    # add the data to it
    try:
        wks = sh.worksheet_by_title(tab_title)
        wks.set_dataframe(data,(1,1))
    except:
        error_flag = True

def read_sql_then_write_gsheet(prov, prod, date_range, sheets_file, tab):

    # sql query
    with sqlEngine.connect() as dbConnection:
        query = select_query(prov, prod, date_range)
        db_results = pd.read_sql(sql=query, con=dbConnection)    

    #add totals row
    total_row = pd.DataFrame([{'date': 'Total',
                                'Clicks': sum(db_results['Clicks']),
                                'Spend': sum(db_results['Spend'])
                                }])
    db_results = pd.concat([db_results, total_row], axis=0)
    
    # to gsheet
    write_to_gsheet(sheets_file, tab, db_results)


# %%
# connections 
sqlEngine = sqlEngineCreator('aircamel_rep_username', 'aircamel_rep_password', 'aircamel_rep_host', 'aircamel_rep_db')
gs_auth = pygsheets.authorize(service_file='../auth/mozo-private-dev-19de22e18578.json')

gsheets = config.cred_info['gsheets']

#%%
# execute
from datetime import date, timedelta

today_date = datetime.date.today()
this_first = today_date.replace(day=1)
prev_last = this_first - timedelta(days=1)
prev_first = prev_last.replace(day=1)

today_string = today_date.strftime("%Y-%m-%d")
this_first_string = this_first.strftime("%Y-%m-%d")
prev_first_string = prev_first.strftime("%Y-%m-%d")

tab_string = today_date.strftime("%B %Y")
prior_tab_string = prev_first.strftime("%B %Y")

month_string = '`date` >= "' + this_first_string + '" and `date` < "' + today_string + '"'
prior_month_string = '`date` >= "' + prev_first_string + '" and `date` < "' + this_first_string + '"'

error_flag = False

try:
    for gs in gsheets:
        # test   gs = gsheets[0]

        prov = gs['provider']
        prod = gs['product_type']
        key = gs['gsheets_key']

        sheets_file = gs_auth.open_by_key(key)

        # if there no tab for this month, do last month
        try:
            sheets_file.worksheets('title', tab_string)
        except:
            read_sql_then_write_gsheet(prov, prod, prior_month_string, sheets_file, prior_tab_string)   

        # then run today's as normal (it will create this month if it isn't there already)
        read_sql_then_write_gsheet(prov, prod, month_string, sheets_file, tab_string)   

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
