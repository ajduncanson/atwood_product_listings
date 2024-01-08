
"""atwood product components

Get response from Impact Radius API
Save as xml file.
Use this script to parse the response records into a csv table.

Run this file from within the /code dir of the project

"""
#%% 
# setup

import os
import pandas as pd
import numpy as np
import datetime
import pytz
import config
import pickle
from sqlalchemy import create_engine
import pygsheets

time_zone = pytz.timezone('Australia/Sydney')
filesavetime = datetime.datetime.now().strftime("%Y%m%d_%H%M")

# file save details
curpath = os.path.abspath(os.curdir)
data_raw_path = curpath + '/../data/raw/'
data_proc_path = curpath + '/../data/processed/'

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
error_flag = False

try:
    for prod in products_types:

        # test    prod = 'HL'
        with sqlEngine.connect() as dbConnection:
            query =f"""select *  from {view_names[prod]};
                    """
            db_results = pd.read_sql(sql=query, con=dbConnection)
            result[prod] = db_results

except:
    error_flag = True

# %%
# save to gsheets

try:

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

except:
    error_flag = True

# %%
# save pickle

pname = data_proc_path + fname + '_' + filesavetime + '.pkl'
with open(pname, 'wb') as file: 
    pickle.dump(result, file) 


# %%
# error check flag
    
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
