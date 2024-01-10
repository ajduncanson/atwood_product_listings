#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
For Daily click report gsheets
Contains details of DB env file and gsheet keys
"""

import os

cred_info={}
dev_env_file = os.path.expanduser('~/.dev_env')

with open(dev_env_file) as f:
    for line in f:
        (key,val) = line.rsplit()[0].split('=')
        cred_info[key]=val
        
cred_info['gsheets'] = [
    {'provider': 'Up',
     'product_type': 'HomeLoan',
     'gsheets_key': '1Lw-p5ocXlLTw8gv9Nw0gXhWq2k_OhWCY6qoTtVjWJo0'}
]
# note that the Sheets files must be shared with pygsheets@mozo-private-dev.iam.gserviceaccount.com

########################################
