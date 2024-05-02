
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
    return query

def select_query_aggregate(dates):

    query = f"""
    select product_type as ProductType, provider_name as Provider, count(*) as Clicks
        from rcd
	where {dates}  
	  and source = 'gts'
	  group by product_type, provider_name
      order by product_type, count(*) desc 
	  ;
    """
    return query


def write_to_gsheet(sh, tab_title, data, with_index, with_format):
    
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
        wks.set_dataframe(data,(1,1), copy_index = with_index)
    except:
        error_flag = True

    if with_format == True:
        # column formatting
        wks.apply_format(ranges=['B:Z'], format_info={"numberFormat": {"type": 'NUMBER', "pattern": "[=0]0;[>0]#,###"}, "horizontalAlignment": 'RIGHT'})

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
    result = pd.DataFrame()
            # result = pd.DataFrame({'Date': date_range, 'temp': date_range})
            # result = result.set_index('Date')
            # result.columns=pd.MultiIndex.from_tuples([('','','','temp')])

    if len(product_types) == 0:
        result['results'] = 0
    else:
        for prod in product_types:

            # test   prod = product_types[0]
        
            this_type = db_results[db_results['ProductType']== prod]
            products = list(set(this_type['Product']))
            products.sort()
            cols = content[prod]['cols']

            # by product record
            if content[prod]['product_level'] == 'product':
                for product in products:

                    # test   product = products[0]

                    this_product = this_type[this_type['Product']== product]

                    # product    
                    this_product = make_single_table(this_product, cols, is_type = False)
                    # re-order columns
                    new_col_index = this_product.columns.reindex(cols, level=3)
                    this_product = this_product.reindex(columns = new_col_index[0]) #new_col_index is a single item tuple
                    # add to result
                    if len(result) == 0:
                        result = this_product
                    else:
                        result = result.merge(right = this_product, on = 'Date', how = 'outer')
            
            # by product_type
            if (content[prod]['product_level'] == 'type' or (content[prod]['product_level'] == 'product' and len(products) > 1)):
                this_type = make_single_table(this_type, cols, is_type = True)
                # re-order columns
                new_col_index = this_type.columns.reindex(cols, level=3)
                this_type = this_type.reindex(columns = new_col_index[0]) #new_col_index is a single item tuple
                # add to result
                if len(result) == 0:
                    result = this_type
                else:        
                    result = result.merge(right = this_type, on = 'Date', how = 'outer')

            # by regex group
            if (content[prod]['product_level'] == 'regex'):

                group1 = this_type[this_type['Product'].str.contains(grouping['regex'])]
                group1 = group1.assign(ProductType = grouping['label'])
                group2 = this_type[~this_type['Product'].str.contains(grouping['regex'])]
                group2 = group2.assign(ProductType = grouping['other_label'])

                if len(group1) >0:
                    this_group_1 = make_single_table(group1, cols, is_type = True)
                    new_col_index = this_group_1.columns.reindex(cols, level=3)
                    this_group_1 = this_group_1.reindex(columns = new_col_index[0]) #new_col_index is a single item tuple

                if len(group2) >0:
                    this_group_2 = make_single_table(group2, cols, is_type = True)
                    new_col_index = this_group_2.columns.reindex(cols, level=3)
                    this_group_2 = this_group_2.reindex(columns = new_col_index[0]) #new_col_index is a single item tuple

                # add to result
                if len(result) == 0 and len(group1) >0:
                    result = this_group_1
                elif len(group1) >0:                  
                    result = result.merge(right = this_group_1, on = 'Date', how = 'outer')
                if len(result) == 0 and len(group2) >0:
                    result = this_group_2
                elif len(group2) >0: 
                    result = result.merge(right = this_group_2, on = 'Date', how = 'outer')

    # tidy up
    result = result.fillna(value=0)
    
    # to gsheet
    write_to_gsheet(sheets_file, tab, result, with_index = True, with_format = True)

    return result

#%%

def extract_transform_load_aggregate(date_string, sheets_file, tab):

    #test   date_string = month_string; tab = tab_string

    # sql query
    with sqlEngine.connect() as dbConnection:
        query = select_query_aggregate(date_string)
        db_results = pd.read_sql(sql=query, con=dbConnection)    

    # tidy up
    result = db_results.fillna(value=0)
    result = result.reset_index(drop=True)
    
    # to gsheet
    write_to_gsheet(sheets_file, tab, result, with_index = False, with_format = False)

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
yesterday_date = today_date - timedelta(days=1)
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

month_range = pd.date_range(start=this_first, end=yesterday_date).date
prior_month_range = pd.date_range(start=prev_first, end=prev_last).date

#%%
# execute

error_flag = False
try:
    for gs in gsheets:
        # test   gs = gsheets[16]

        prov = gs['provider']
        content = gs['content']
        key = gs['gsheets_key']
        if 'grouping' in gs.keys():
            grouping = gs['grouping']

        sheets_file = gs_auth.open_by_key(key)

        # if there no tab for this month, do last month
        try:
            sheets_file.worksheets('title', tab_string)
        except:
            if prov == 'All':
                extract_transform_load_aggregate(prior_month_string, sheets_file, prior_tab_string)   
            else:
                extract_transform_load(prov, content, prior_month_string, prior_month_range, sheets_file, prior_tab_string)   

        # then run today's as normal (it will create this month if it isn't there already)
        if prov == 'All':
            extract_transform_load_aggregate(month_string, sheets_file, tab_string)   
        else:
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
