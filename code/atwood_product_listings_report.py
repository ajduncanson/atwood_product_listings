
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
import pickle
from sqlalchemy import create_engine
import pygsheets

import atwood_product_listings_report_config as config

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
gsheet_key = config.cred_info['gsheet_key']

# %%
# connection to current db list of endpoints
sqlEngine = sqlEngineCreator('ethercat_username', 'ethercat_password', 'ethercat_host', 'ethercat_db')

#%%
# get views from DB


prod_table = {'HomeLoan': 'home_loans',
              'SavingsAccount': 'savings_accounts',
              'PersonalLoan': 'personal_loans',
              'CarLoan': 'personal_loans',
              'TermDeposit': 'term_deposits',
              'CarInsurance': 'car_insurances'}

def select_query(prod):
    query = f"""
    select distinct t.product_id AS product_id, t.provider AS provider, t.product_name AS product_name,
    t.page_last_updated AS page_last_updated, t.recency AS recency, t.page_link AS page_link,
    t.page_author, t.last_updater, t.atwood_template
    from 
    (select `a`.`id` AS `product_id`,`prov`.`name` AS `provider`,`a`.`name` AS `product_name`,
    (case when (`p`.`last_updated_at` is null) then `p`.`published_at` else `p`.`last_updated_at` end) AS `page_last_updated`,
    (case when ((`p`.`last_updated_at` is null) or ((to_days(now()) - to_days(`p`.`last_updated_at`)) < 180)) then 3 else 1 end) AS `recency`,
    concat('https://mozo.com.au',`p`.`path`) AS `page_link`,
    ca.name AS page_author,
    au.name AS last_updater,
    ct.name AS atwood_template
    from 
    (`ferris_production`.`cms_entities` `e` 
    left join `ferris_production`.`cms_pages` `p` on (`e`.`cms_page_id` = `p`.`id`)

    left join cms_authors_pages cap  
    on p.id = cap.cms_page_id    
    
    left join cms_authors ca  
    on cap.cms_author_id = ca.id
    
    left join atwood_users au
    on p.updater_id = au.id

    left join cms_templates ct 
    on p.cms_template_id = ct.id

    left join `ferris_production`.`cms_component_entities` `ce` on (`e`.`cms_component_entity_id` = `ce`.`id`) 
    left join (
        select `ferris_production`.`cms_entities`.`cms_component_entity_id` AS `id`, 
        1 AS `flag` 
        from `ferris_production`.`cms_entities` 
        where ((`ferris_production`.`cms_entities`.`cms_component_prop_id` = 81) 
            and (`ferris_production`.`cms_entities`.`published_content` = '{prod}'))
        ) `entity_list` 
        on (`e`.`cms_component_entity_id` = `entity_list`.`id`)
    )
    left join `ferris_production`.`{prod_table[prod]}` `a` on(`e`.`published_content` = `a`.`id`) 
    left join `ferris_production`.`providers` `prov` on (`a`.`provider_id` = `prov`.`id`)  
    where ((`e`.`cms_component_prop_id` = 83) and (`e`.`published_content` is not null) and (`p`.`is_published` = 1) and (`ce`.`hidden` = 0) and (`entity_list`.`flag` = 1)) 
    order by `recency` desc,`prov`.`name`,`a`.`name`,`e`.`cms_page_id`) `t`
    """
    return query


### when ((`p`.`last_updated_at` is null) or ((to_days(now()) - to_days(`p`.`last_updated_at`)) < 365)) then 2

#%%

products_types = ['HomeLoan', 'SavingsAccount', 'CarLoan'] #, 'PL', 'TD']

result = dict()
error_flag = False

try:
    for prod in products_types:

        # test    prod = 'HomeLoan'
        with sqlEngine.connect() as dbConnection:
            query = select_query(prod)
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
    for prod in products_types:
        wks = sh.worksheet_by_title(prod + ' raw data')
        wks.set_dataframe(result[prod],(1,1))

    #updated the latest date
    wks = sh.worksheet_by_title('cover')
    wks.update_value((2,3), filesavetime)

except:
    error_flag = True

# %%
# save pickle

fname = 'atwood_product_listings'
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
