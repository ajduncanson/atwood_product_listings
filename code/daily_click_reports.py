
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

def select_query(provider, dates):

    query = f"""
    select `date` as Date, product_type as ProductType, product_name as Product, count(*) as Clicks, sum(converted) as Apps, sum(earnings_per_e2e) as Spend
        from rcd
	where {dates}  
	  and source = 'gts'
	  and provider_name = '{provider}'
	  group by `date`, product_type, product_name	  
	  ;
    """

    ### Not using Apps and Spend right now:     , sum(converted) as Apps, sum(earnings_per_e2e) as Spend

    return query

def write_to_gsheet(sh, tab_title, data):
    
# test      sh = sheets_file; tab_title = tab; data = result

    # get the right tab, or make one
    try:
        # get tab
        wks = sh.worksheet_by_title(tab_title)
    except:
        # make tab
        try:
            sh.add_worksheet(tab_title, rows=100, cols=26, index = 0)
            wks = sh.worksheet_by_title(tab_title)
        except:
            error_flag = True
    
    # clear worksheet and add new data to it
    try:
        wks = sh.worksheet_by_title(tab_title)
        wks.clear(start='A1', end=None, fields='*')
        wks.set_dataframe(data,(1,1), copy_index = True)
    except:
        error_flag = True

    # column formatting
    wks.apply_format(ranges=['B:Z'], format_info={'numberFormat': {'type': 'NUMBER'}, "horizontalAlignment": 'RIGHT'})

    # I built this to format Spend columns but we are not using those
    # for c in ['D:D','G:G','J:J','M:M','P:P','S:S','V:V','Y:Y','AB:AB','AE:AE','AH:AH']:
    #     try:
    #         wks.apply_format(ranges=[c], format_info={'numberFormat': {'type': 'CURRENCY'}})
    #     except:
    #         None

    #row formatting
    wks.apply_format(ranges=['3:3'], format_info={"wrapStrategy": 'WRAP', "horizontalAlignment": 'RIGHT'})

def make_single_table(data, content_columns, is_type):
    if is_type == True:
        id_list = ['Date', 'ProductType']
        col_list = ['ProductType','variable']
    else:
        id_list = ['Date', 'ProductType', 'Product']
        col_list = ['ProductType', 'Product', 'variable']

    melted = pd.melt(data, id_vars=id_list, value_vars= content_columns)  ### Not using Apps and Spend right now:    , 'Apps', 'Spend'])
    pivoted = pd.pivot_table(melted, index = ['Date'], columns=col_list, aggfunc='sum', fill_value=0, dropna=True,  margins = True, margins_name= 'TOTAL', sort=True)

    # remove the total column if the margins function has created one, but keep the total row
    if 'TOTAL' in pivoted.columns.get_level_values(level = 1):
        pivoted = pivoted.drop(labels = 'TOTAL', axis = 1, level = 1)

    # now align multiIndex levels so product and total tables may be merged
    if is_type == True:
        pivoted.columns = pd.MultiIndex.from_tuples([(prov, e[1], 'TOTAL', e[2]) for e in pivoted.columns])
    else:
        pivoted.columns = pd.MultiIndex.from_tuples([(prov, e[1], e[2], e[3]) for e in pivoted.columns])

    return(pivoted)


def extract_transform_load(prov, content, date_string, date_range, sheets_file, tab):

    #test   date_string = month_string; date_range=month_range; tab = tab_string

    # sql query
    with sqlEngine.connect() as dbConnection:
        query = select_query(prov, date_string)
        db_results = pd.read_sql(sql=query, con=dbConnection)    

    product_types = list(set(db_results['ProductType']))
    product_types = [t for t in product_types if t in content.keys()]
    product_types.sort()

    # create an empty, with the right index and MultiIndex columns to be able to merge products
    result = pd.DataFrame({'Date': date_range, 'temp': date_range})
    result = result.set_index('Date')
    result.columns=pd.MultiIndex.from_tuples([('','','','temp')])

    for prod in product_types:

        # test   prod = product_types[0]
    
        this_type = db_results[db_results['ProductType']== prod]
        products = list(set(this_type['Product']))
        products.sort()
        cols = content[prod]['cols']

        if content[prod]['product_level'] == True:
            for product in products:

                # test   product = products[0]

                this_product = this_type[this_type['Product']== product]

                # product    
                this_product = make_single_table(this_product, cols, is_type = False)
                # re-order columns
                new_col_index = this_product.columns.reindex(cols, level=3)
                this_product = this_product.reindex(columns = new_col_index[0]) #new_col_index is a single item tuple
                # add to result
                result = result.merge(right = this_product, on = 'Date', how = 'outer')
        
        # product_type
        if (content[prod]['product_level'] == False or len(products) > 1):
            this_type = make_single_table(this_type, cols, is_type = True)
            # re-order columns
            new_col_index = this_type.columns.reindex(cols, level=3)
            this_type = this_type.reindex(columns = new_col_index[0]) #new_col_index is a single item tuple
            # add to result
            result = result.merge(right = this_type, on = 'Date', how = 'outer')

    # tidy up
    result = result.fillna(value=0)
    result = result.drop(columns = [('','','','temp')])
    
    # to gsheet
    write_to_gsheet(sheets_file, tab, result)

    return result

# %%
# connections 
sqlEngine = sqlEngineCreator('aircamel_rep_username', 'aircamel_rep_password', 'aircamel_rep_host', 'aircamel_rep_db')
gs_auth = pygsheets.authorize(service_file='../auth/mozo-private-dev-19de22e18578.json')

gsheets = config.cred_info['gsheets']

#%%
# set date ranges

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

month_range = pd.date_range(start=this_first, end=today_date).date
prior_month_range = pd.date_range(start=prev_first, end=prev_last).date

#%%
# execute

error_flag = False
try:
    for gs in gsheets:
        # test   gs = gsheets[0]

        prov = gs['provider']
        content = gs['content']
        key = gs['gsheets_key']

        sheets_file = gs_auth.open_by_key(key)

        # if there no tab for this month, do last month
        try:
            sheets_file.worksheets('title', tab_string)
        except:
            extract_transform_load(prov, content, prior_month_string, prior_month_range, sheets_file, prior_tab_string)   

        # then run today's as normal (it will create this month if it isn't there already)
        extract_transform_load(prov, content, month_string, month_range, sheets_file, tab_string)   

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
