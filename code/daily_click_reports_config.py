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
    #test file
    {'provider': 'Ubank',
     'gsheets_key': '1Lw-p5ocXlLTw8gv9Nw0gXhWq2k_OhWCY6qoTtVjWJo0'}
    #live files
    #, {'provider': 'OurMoneyMarket',
    #  'gsheets_key': '1IIqNKIm7J9izhmSHWlMKWq06rWC'},
    # {'provider': 'Honey Insurance',
    #  'gsheets_key': '1nhz0sp-y3tp8caPJB8CF-RBdC1iAmJCAHe5Af9sxe_s'},
    # {'provider': 'Harmoney',
    #  'gsheets_key': '1QVjVDXhTx-9No8WR3gItG_GpVKvmbD35AcMdryjyANo'},
    # {'provider': 'AMP Bank',
    #  'gsheets_key': '1zcWeCHy8q8EwcAb492pEsJNL8cSoVyxjbsd-63hjB08'},
    # {'provider': 'Lendi',
    #  'gsheets_key': '1fMYGVXwxUKmbvW6gVx50l7cvSNPtW17CEb1h6S0MGU0'},
    # {'provider': 'Ubank',
    #  'gsheets_key': '1f4qC3I9-TWnFA41ZjslWtYw45COX_Oim5MABYRKrjNo'},
    # {'provider': 'Freely',
    #  'gsheets_key': '1rL_8mIOvqAjGrEaQFbiWCxFoikhdQZGQHxJ0d5dca0g'},
    # {'provider': 'NRMA',
    #  'gsheets_key': '1e6M4XxeSYbWhf-8ISjd3_IS24vyFD59omTtEkNKmQhQ'}
]

# note that the Sheets files must be shared with pygsheets@mozo-private-dev.iam.gserviceaccount.com

########################################
