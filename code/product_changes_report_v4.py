
"""product changes report

get products that have had changes made or scheduled, 
identify where they are inserted into Atwood pages and changes are required
and populate gsheets

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

pd.set_option('display.max_rows', None)

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


def get_pfv(datetime1):

    query = f"""
    select DATE(updated_at) as updated_at,  
    product_type, product_id, 
    field_name as changes, 
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(field_value, '(---( )?)', ''),'\\n','; '),''\';', '') as new_value,
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(previous_field_value, '(---( )?)', ''),'\\n','; '),''\';', '') as previous_value,
    scheduled_at, updated_at as added_to_admin,
    'mozo.com.au' as product_page_link
    from pending_field_values
    where updated_at > '{datetime1}'
    and product_type <> 'MppTab'
    and field_name <> 'gts'
    and state = 'updated'
    """
    
    with sqlEngine_spacecoyote.connect() as dbConnection:
        result = pd.read_sql(sql=query, con=dbConnection)

        # The above excludes:
            # pfv changes that have 'state' = 'pending' with 'scheduled_at' as 'NULL'  as these have not been committed yet
            # pfv changes that have 'state' = 'canceled' (yes this is spelt incorrectly)
        # But does not allow for situations where we schedule a change and then subsequently change the state to canceled.. it may already be in the change report.

    return result


def get_product_set(datetime1):

  ### before PPT editorial reviews, we only looked for monetised ones   
    # query = f"""
    # select product_type, product_id
    # from gts_link_versions
    # where created_at > '{datetime1}'
    # and active = 1   
    # UNION
    # select product_type, product_id 
    # from gts_links
    # where active = 1
    # and right(product_type, 7) <> 'Version'
    # """
    
  ### now we get the whole product list
    query = f"""
    select product_id, product_type, product_name, provider_id,
       provider_name as provider, product_group_id, product_group_name
    from product_master_list
    """

    with sqlEngine_ethercat.connect() as dbConnection:
        result = pd.read_sql(sql=query, con=dbConnection)  

    return result

def get_product_name_ids(dict):
    ### before PPT editorial reviews, we only looked for monetised ids
    ### and this function was used later
    ### no longer needed; all this is from product_master_list now!

    all_products = pd.DataFrame()

    for k,v in dict.items():
        query = f"""
        select '{k}' as product_type, 
        a.provider_id, a.id as product_id, a.product_group_id, pg.name as product_group_name,
        p.name as provider, a.name as product_name
        from {table_name(k, version = False)} a
        left join providers p
        on a.provider_id = p.id
        left join product_groups pg
        on a.product_group_id = pg.id
        where a.id in {'(' + ','.join([str(e) for e in v]) + ')'}
        """
        with sqlEngine_ethercat.connect() as dbConnection:
            this_one = pd.read_sql(sql=query, con=dbConnection) 
        all_products = pd.concat([all_products, this_one])

    return all_products

def make_product_page_url(product_type, provider, product_group, product_group_id, id):

    product_page_dict = {'HomeLoan': '/home-loans/information',
                         'TermDeposit': '/term-deposits/information', 
                         'SavingsAccount': '/savings-accounts/information', 
                         'BankAccount': '/bank-accounts/information', 
                         'PersonalLoan': '/personal-loans/information', 
                         'CreditCard': '/credit-cards/information',
                         'CarInsurance': '/car-insurance/information',
                         'HomeInsurance': '/home-insurance/information',
                         'TravelInsurance': '/travel-insurance/information',
                         'PetInsurance': '/pet-insurance/information',
                         'ShareAccount': '/share-trading/information',
                         'PrepaidTravelCard': '/travel-money/prepaid-travel-cards',
                         'MarginLoan': '/margin-loans',
                         'BusinessLoan': '/small-business/business-loans/information'
                         }
    
    no_ppt_page_dict = {'InternationalMoneyTransfer': '/international-money-transfer'
                                        } 
    
    provider = provider.replace(' ', '-').lower()
    product_group = product_group.replace(' ', '-').lower()

    if product_type in product_page_dict.keys():
        result = 'mozo.com.au' + product_page_dict[product_type] + '/' +  provider # old prod page format:   + '/' + product_group + '/' + str(product_group_id) + '?id=' + str(id)
    elif product_type in no_ppt_page_dict.keys():
        result = 'mozo.com.au' + no_ppt_page_dict[product_type]
    else:
        result = ' '
    return result

### old version before PPT considlation
# def make_product_page_url(product_type, provider, product_group, product_group_id, id):

#     product_page_dict = {'HomeLoan': '/home-loans/information',
#                          'TermDeposit': '/term-deposits/information', 
#                          'SavingsAccount': '/savings-accounts/information', 
#                          'BankAccount': '/bank-accounts/information', 
#                          'PersonalLoan': '/personal-loans/information', 
#                          'CreditCard': '/credit-cards/information',
#                          'CarInsurance': '/insurance/car-insurance',
#                          'HomeInsurance': '/insurance/home-insurance',
#                          'TravelInsurance': '/insurance/travel-insurance',
#                          'ShareAccount': '/share-trading',
#                          'PrepaidTravelCard': '/travel-money/prepaid-travel-cards',
#                          'MarginLoan': '/margin-loans',
#                          'BusinessLoan': '/small-business/business-loans/information'
#                          }
    
#     product_page_whole_provider_dict = {'PetInsurance': '/insurance/pet-insurance',
#                                         'InternationalMoneyTransfer': '/international-money-transfer/resources/providers'
#                                         } 
    
#     provider = provider.replace(' ', '-').lower()
#     product_group = product_group.replace(' ', '-').lower()

#     if product_type in product_page_dict.keys():
#         result = 'mozo.com.au' + product_page_dict[product_type] + '/' +  provider + '/' + product_group + '/' + str(product_group_id) + '?id=' + str(id)
#     elif product_type in product_page_whole_provider_dict.keys():
#         result = 'mozo.com.au' + product_page_whole_provider_dict[product_type] + '/' +  provider
#     else:
#         result = ' '
#     return result


def write_to_gsheet(sh, tab_title, data):
    
    # test      sh = sheets_file; tab_title = 'details'; data = filtered_details
    # test      sh = sheets_file; tab_title = 'worklist'; data = worklist
  
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


#%%
# set date ranges

from datetime import date, datetime, timedelta

run_time = datetime.now()
run_timestamp = run_time.strftime("%Y-%m-%d %H:%M")


#%%
# execute

error_flag = False
try:
    gs = gsheets[0]

    key = gs['gsheets_key']
    sheets_file = gs_auth.open_by_key(key)

    # look up the added_to_admin date of the most recent date written to gsheets
    wks = sheets_file.worksheet('title','details')
    cells = wks.get_col(col = 9,returnas='matrix', include_tailing_empty=False)
    cells_times = [datetime.strptime(c, "%Y-%m-%d %H:%M:%S") for c in cells[1:]]

    # use that timestamp as the starting point for this query
    ##test pfv_date = '2024-05-07 08:00:00'

    pfv_date = datetime.strftime(max(cells_times), "%Y-%m-%d %H:%M:%S")

    # recent gts date = 40 days prior
    gts_date = (datetime.strptime(pfv_date, "%Y-%m-%d %H:%M:%S") - timedelta(days=40)).strftime("%Y-%m-%d")

    # EXTRACT 1: find all the pending field value records
    data_pfv = get_pfv(pfv_date)

    # EXTRACT 2: find all the products (previously, just monetised ones)
    data_products = get_product_set(gts_date)


  ### not required now we want non-monetised also
    # # LOOP EXTRACT PRODUCT & PROVIDER NAMES
    # product_dict = data_gts.groupby('product_type')['product_id'].apply(list).to_dict()

    # # remove unwanted product types
    # product_dict = {key: val for key, 
    #         val in product_dict.items() if key != 'Electricity'}

    # monetised_products = get_product_name_ids(product_dict)

    # TRANSFORM
    # no NaNs
    data_pfv = data_pfv.fillna(value=0)
    data_pfv['run_timestamp'] = run_time

    # JOIN
    result = pd.merge(left = data_pfv, right = data_products, how = 'inner', on = ['product_type', 'product_id'])

    # tidy ups
    result = result.reset_index(drop=True)
    result = result.sort_values(by = ['updated_at', 'product_type', 'provider', 'product_name', 'changes', 'added_to_admin'], axis = 0)
    col_order = ['updated_at', 'product_type', 
                    'provider', 'product_name',
                    'changes', 
                    'new_value', 
                    'previous_value',
                    'scheduled_at', 'added_to_admin',
                    'run_timestamp', 
                    'provider_id', 'product_group_id', 'product_group_name', 'product_id', 'product_page_link']
    result = result[col_order]


# create the product_page_link
    
    result['product_page_link'] = [
        make_product_page_url(row['product_type'], row['provider'], row['product_group_name'], row['product_group_id'], row['product_id']) 
        for i,row in result.iterrows()
        ]

    
# bring in the latest atwood products csv
    
    atwood = pd.read_csv(curpath + '/../data/atwood_products/atwood_products_latest.csv')

# create list of products with data changes

    join_cols = ['product_type', 'product_id']
    changes = result.drop_duplicates(subset=join_cols)[join_cols]

    changed_fields = None

    for i, row in changes.iterrows():
        
        row_df = pd.DataFrame(row).transpose()
        subset = row_df.merge(right=result, on = join_cols)
        changed = ', '.join(subset['changes'])
        if changed is None:
            changed = ' '
        if changed_fields is None:
            changed_fields = [changed]
        else:
            changed_fields.append(changed)

    changes['changed_fields'] = changed_fields

# join atwood listings and products with changes to make worklist

    #merge
    worklist = atwood.merge(right = changes, how = 'left', on = join_cols)

    #drop anything without changes
    worklist = worklist.dropna(axis = 0, subset=['changed_fields'])

    #drop anything not recent enough
    worklist = worklist[worklist['recency']==3]

    #add in gts_link info
    worklist['gts_live'] = 'tbc'

    #get provider and product names


    #specify final report columns & sort

    report_cols = join_cols + ['changed_fields', 'page_link', 'page_author', 'gts_live', 
                            #    'pageviews_7_days', 
                               'page_last_updated']
    worklist = worklist[report_cols]

    #join rows where mulitple authors
    worklist['page_author'] = worklist.groupby([r for r in report_cols if r not in ['page_author']])['page_author'].transform(lambda x: ','.join(x))
    worklist = worklist.drop_duplicates(ignore_index=True)

    worklist = worklist.sort_values(by = ['product_type', 'product_id', 'gts_live'
                                                    #   , 'pageviews_7_days'
                                                      ],
                                    ascending = [True, True, False
                                                # , False
                                                 ])
    
    worklist['run_time'] = run_time


# make detail list including only the atwood worklist products
    
    #merge
    filtered_details = result.merge(right = worklist, how = 'left', on = join_cols)

    #drop anything not in worklist
    filtered_details = filtered_details.dropna(axis = 0, subset=['page_link'])
    drop_cols = ['changed_fields', 'page_link', 'page_author',
       'gts_live', 
    #    'pageviews_7_days', 
       'page_last_updated', 'run_time']
    filtered_details = filtered_details.drop(columns=drop_cols)
    filtered_details = filtered_details.drop_duplicates(ignore_index=True)

# change datetimes into strings, for writing to files (added to admin needs seconds, as we use it as the starting point next time!)
    
    for c in ['run_time']:
        worklist[c] = [t.strftime("%Y-%m-%d %H:%M") for t in worklist[c]]
    for c in ['scheduled_at', 'run_timestamp']:
        filtered_details[c] = [t.strftime("%Y-%m-%d %H:%M") if t != 0 else '' for t in filtered_details[c]]
    for c in ['added_to_admin']:
        filtered_details[c] = [t.strftime("%Y-%m-%d %H:%M:%S") if t != 0 else '' for t in filtered_details[c]]

    filtered_details['updated_at'] = [t.strftime("%Y-%m-%d") if t != 0 else '' for t in filtered_details['updated_at']]

# write to worklist gsheet
    
    write_to_gsheet(sheets_file, 'worklist', worklist)
    write_to_gsheet(sheets_file, 'details', filtered_details)

    number_of_pages = len(worklist)
    number_of_changes = len(filtered_details)

# set error flag if the above failed
    
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
        # f = open(data_proc_path + "success_date.txt", "a")
        # f.write('TBC')
        # f.close()
    
    # send to webhook to trigger slack message
    if number_of_pages > 0:
        slack_webhook = 'https://hooks.slack.com/triggers/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'
        body = '{"changes": "' + str(number_of_changes) + '", "pages": "' + str(number_of_pages) + '", "timestamp": "' + run_timestamp +'"}'
        r = requests.post(url=slack_webhook, data=body)


# %%

