#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
For product changes report gsheets
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
    
    {'gsheets_key': '1qLtP9RBkrX-0WM4m6aLluZnYUiTSsitPlucnCG2jrio'
    },
    {'gsheets_key': '1zCwg4ef7Jrq8HvGKShPJLaWmgy3mRW5-dfxLq5cSNdI'}
]  


# note that the Sheets files must be shared with pygsheets@mozo-private-dev.iam.gserviceaccount.com

########################################
