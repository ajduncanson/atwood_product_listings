#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""atwood product components

Get response from Impact Radius API
Save as xml file.
Use this script to parse the response records into a csv table.

Run this file from within the /code dir of the project

"""
#%%
# sentry - server error reporting
import sentry_sdk
sentry_sdk.init(
    dsn="https://2179fc03f24248cfae662cb7fd31df31@o69459.ingest.sentry.io/6778340",

    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    # We recommend adjusting this value in production.
    traces_sample_rate=1.0
)
#%% 
# setup

import pandas as pd
import numpy as np
import datetime
import pytz
import os
import config
from sqlalchemy import create_engine

from dotenv import load_dotenv
# package to pip install is called python-dotenv !




curpath = os.path.abspath(os.curdir)
time_zone = pytz.timezone('Australia/Sydney')
filesavetime = datetime.datetime.now().strftime("%Y%m%d_%H%M")


# file save details
data_raw_path = curpath + '/../data/raw/'
data_proc_path = curpath + '/../data/processed/'


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
view_names = config.cred_info['view_names']
gsheet_key = config.cred_info['gsheet_key']

# %%
# connection to current db list of endpoints
sqlEngine = sqlEngineCreator('ethercat_username', 'ethercat_password', 'ethercat_host', 'ethercat_db')

#%%
# get views from DB

products_types = ['HL', 'SA'] #, 'PL', 'TD']

result = dict()
fname = 'atwood_product_listings'

for prod in products_types:

    # test    prod = 'HL'
    with sqlEngine.connect() as dbConnection:
        query =f"""select *  from {view_names[prod]};
                """
        db_results = pd.read_sql(sql=query, con=dbConnection)
        result[prod] = db_results
        db_results.to_csv(data_proc_path + fname + '_' + prod + '_' + filesavetime + '.csv', index=True)


# %%
# save to gsheets

import pygsheets

#authorization
gc = pygsheets.authorize(service_file='../auth/mozo-private-dev-19de22e18578.json')
sh = gc.open_by_key(gsheet_key)
# note that the Sheets file must be shared with pygsheets@mozo-private-dev.iam.gserviceaccount.com

#update the raw data sheets, starting at cell A1. 
wks = sh.worksheet_by_title('HL raw data')
wks.set_dataframe(result['HL'],(1,1))
wks = sh.worksheet_by_title('SA raw data')
wks.set_dataframe(result['SA'],(1,1))


#updated the latest date
wks = sh.worksheet_by_title('cover')
wks.update_value((2,3), filesavetime)


# %%
