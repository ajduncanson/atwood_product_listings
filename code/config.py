#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
For Atwood product listing report
Contains details of DB env file and tables
"""

import os

cred_info={}
dev_env_file = os.path.expanduser('~/.dev_env')

with open(dev_env_file) as f:
    for line in f:
        (key,val) = line.rsplit()[0].split('=')
        cred_info[key]=val
        

cred_info['view_names'] = {'HL': 'view_Atwood_product_listings_HL',
                           'SA': 'view_Atwood_product_listings_SA'
                           }


#'PL' 'TD' 'CC' 'BL' 'ST' each insurance, IMT




cred_info['gsheet_key'] = '1fG7lWWkvwZrlNKj1rP5ERwQwkcdA40w97e170Iih700'

########################################
