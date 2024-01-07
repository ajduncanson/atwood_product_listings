#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""impact radius json parse

Get response from Impact Radius API
Save as xml file.
Use this script to parse the response records into a csv table.

Run this file from within the /code dir of the project

"""

#%% 
# setup

import pandas as pd
import numpy as np
import json
import xmltodict
import datetime
import pytz
import requests
import os
from dotenv import load_dotenv
# package to pip install is called python-dotenv !

curpath = os.path.abspath(os.curdir)
time_zone = pytz.timezone('Australia/Sydney')

# file save details
data_raw_path = curpath + '/../data/raw/'
data_proc_path = curpath + '/../data/processed/'


#%% 
# requests.get

# Here's what our current rake task uses:
#report_url = 'https://api.impact.com/Mediapartners/IRDEwun9LdKk1191226d9dsq3ysLqaFGP1/Reports/mp_action_listing_sku?PageSize=1000&Page=1'

# Here's one where we include the "Category List" variable that shows which product was converted, 
# and has custom date range control.
# Reports like this are configured in the Impact Radius portal https://app.impact.com/login.user using the Donny Ha login

report_url_base_1 = 'https://api.impact.com//Mediapartners/'
report_url_base_2 = '/Reports/mp_action_listing_sku?SHOW_SKU=1&timeRange=CUSTOM'

# last 60 days up to and including yesterday

api_get_datetime = datetime.datetime.now(tz=time_zone)

api_get_time = api_get_datetime.strftime("%Y%m%d_%H%M")
custom_end_date = (api_get_datetime - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
custom_start_date = (api_get_datetime - datetime.timedelta(days=60)).strftime("%Y-%m-%d")

headers = {'Content-Type': 'json/application'}

# take environment variables from .env.
load_dotenv()

#%%

impact_accounts = ['1191226', '1208465', '464502', '1196301', '1208463']

result = pd.DataFrame()

for impact_account in impact_accounts:

    # test    impact_account = '1208465'

    user = os.getenv('API_USER_'+ impact_account)
    passwd = os.getenv('API_PASSWORD_'+ impact_account)

    report_url =  report_url_base_1 + user + report_url_base_2 + '&START_DATE=' + custom_start_date + '&END_DATE=' + custom_end_date
    print('The time in Sydney is ' + api_get_time)
    print(report_url)

    response = requests.get(url = report_url, headers = headers, auth = (user, passwd) )
    response_text = response.text

    # convert xml to dict to str, then to json
    data_dict = xmltodict.parse(response_text)
    json_str = json.dumps(data_dict)
    json_data = json.loads(json_str)

    if 'Record' in json_data['ImpactRadiusResponse']['Records'].keys() and '@end' in json_data['ImpactRadiusResponse']['Records'].keys():
        if int(json_data['ImpactRadiusResponse']['Records']['@end']) > 0:
                # create df
                record_list = json_data['ImpactRadiusResponse']['Records']['Record']
                dict = {'mozo_id': [r['SubId1'] for r in record_list], 
                        'action_id': [r['Action_Id'] for r in record_list],
                        'action_timestamp': [r['Action_Date'] for r in record_list], 
                        'campaign': [r['Campaign'] for r in record_list], 
                        'detail': [str(r['SKU']) + " " + str(r['Item_Name']) +  " : " + str(r['Category']) if r['Item_Name'] is not None else str(r['SKU']) +  " : " + str(r['Category']) for r in record_list],
                        'event_type': [r['Event_Type'] for r in record_list], 
                        'status': [r['Status'] for r in record_list],
                        'payout': [r['Payout'] for r in record_list]
                        }        
                df = pd.DataFrame(dict)
                df['impact_account'] = impact_account
                result = pd.concat([result, df])


#%%
# format and summarise

# format and re-order cols
result['payout'] = pd.to_numeric(result['payout'])
result['conversion_date'] = [datetime.datetime.strptime(t[0:10],'%Y-%m-%d') for t in result['action_timestamp']]
first_col = result.pop('conversion_date')	
result.insert(0, 'conversion_date', first_col)

# filter by payout > 0
# (also excludes Deposits records, which are CPC and have $0 payout in this report)
conversions = result[result['payout'] > 0]

# order by date asc
conversions = conversions.sort_values(by = ['conversion_date','campaign', 'detail', 'payout','action_timestamp'], axis=0, ascending=True)

# create summary
summary = pd.DataFrame(conversions.value_counts(['conversion_date', 'campaign', 'detail','status']).sort_index(), columns=['conversions'])

#%% 
# save df to csv

#dfname = 'impact-radius-results-raw'
#csv_time = datetime.datetime.now(time_zone).strftime('%Y%m%d_%H%M')
#df.to_csv(data_proc_path + dfname + '_' + api_get_time + '_parsed_' + csv_time + '.csv')

convname = 'impact-conversions-combined'
summary.to_csv(data_proc_path + convname + '_' + api_get_time + '.csv', index=True)
summary.to_csv(data_proc_path + convname + '-report.csv', index=True)
### END


# %%
