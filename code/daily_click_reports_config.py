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

    {'provider': 'Lendi',
     'content': {'HomeLoanBrokerReferral': {'cols': ['Clicks', 'Spend'], 'product_level': True}
                 },
     'gsheets_key': '1Lw-p5ocXlLTw8gv9Nw0gXhWq2k_OhWCY6qoTtVjWJo0'},

    {'provider': 'Harmoney',
     'content': {'PersonalLoan': {'cols': ['Clicks', 'Spend'], 'product_level': True}
                 },
     'gsheets_key': '1Lw-p5ocXlLTw8gv9Nw0gXhWq2k_OhWCY6qoTtVjWJo0'},

    {'provider': 'Ubank',
     'content': {'HomeLoan': {'cols': ['Clicks', 'Spend'], 'product_level': False},
                 'SavingsAccount': {'cols': ['Clicks', 'Apps', 'Spend'], 'product_level': False},
                 'BankAccount': {'cols': ['Clicks', 'Apps', 'Spend'], 'product_level': False}
                 },
     'gsheets_key': '1Lw-p5ocXlLTw8gv9Nw0gXhWq2k_OhWCY6qoTtVjWJo0'},

    {'provider': 'Bendigo Bank',
     'content': {'HomeLoan': {'cols': ['Clicks', 'Apps', 'Spend'], 'product_level': False},
                 'BankAccount': {'cols': ['Clicks', 'Spend'], 'product_level': False},
                 'SavingsAccount': {'cols': ['Clicks', 'Spend'], 'product_level': False}
                 },
     'gsheets_key': '1Lw-p5ocXlLTw8gv9Nw0gXhWq2k_OhWCY6qoTtVjWJo0'},

    {'provider': 'OurMoneyMarket',
     'content': {'PersonalLoan': {'cols': ['Clicks', 'Spend'], 'product_level': False},
                 'CarLoan': {'cols': ['Clicks', 'Spend'], 'product_level': False},
                 },
     'gsheets_key': '1Lw-p5ocXlLTw8gv9Nw0gXhWq2k_OhWCY6qoTtVjWJo0'},

     {'provider': 'honey',
     'content': {'HomeInsurance': {'cols': ['Clicks', 'Spend'], 'product_level': False}
                 },
     'gsheets_key': '1Lw-p5ocXlLTw8gv9Nw0gXhWq2k_OhWCY6qoTtVjWJo0'},

     {'provider': 'AMP Bank',
     'content': {'BankAccount': {'cols': ['Clicks', 'Spend'], 'product_level': False}
                 },
     'gsheets_key': '1Lw-p5ocXlLTw8gv9Nw0gXhWq2k_OhWCY6qoTtVjWJo0'},

     {'provider': 'Freely',
     'content': {'TravelInsurance': {'cols': ['Clicks', 'Spend'], 'product_level': False}
                 },
     'gsheets_key': '1Lw-p5ocXlLTw8gv9Nw0gXhWq2k_OhWCY6qoTtVjWJo0'},

     {'provider': 'Virgin Money',
     'content': {'CarInsurance': {'cols': ['Clicks'], 'product_level': False}
                 },
     'gsheets_key': '1Lw-p5ocXlLTw8gv9Nw0gXhWq2k_OhWCY6qoTtVjWJo0'}  

]  


    #live files
    # {'provider': 'Lendi',
    #  'gsheets_key': '1fMYGVXwxUKmbvW6gVx50l7cvSNPtW17CEb1h6S0MGU0'},
    # {'provider': 'Harmoney',
    #  'gsheets_key': '1QVjVDXhTx-9No8WR3gItG_GpVKvmbD35AcMdryjyANo'},
    # {'provider': 'Ubank',
    #  'gsheets_key': '1f4qC3I9-TWnFA41ZjslWtYw45COX_Oim5MABYRKrjNo'},
    #, {'provider': 'Bendigo Bank',
    #  'gsheets_key': '1p3oa5iWC7ntf_BvDq6mqbRhHEzVvqDWpINRiFAuV5-g'},   
    #, {'provider': 'OurMoneyMarket',
    #  'gsheets_key': '1IIqNKIm7J9izhmSHWlMKWq06rWC'},
    # {'provider': 'Honey Insurance',
    #  'gsheets_key': '1nhz0sp-y3tp8caPJB8CF-RBdC1iAmJCAHe5Af9sxe_s'},
    # {'provider': 'AMP Bank',
    #  'gsheets_key': '1zcWeCHy8q8EwcAb492pEsJNL8cSoVyxjbsd-63hjB08'},
    # {'provider': 'Freely',
    #  'gsheets_key': '1rL_8mIOvqAjGrEaQFbiWCxFoikhdQZGQHxJ0d5dca0g'},
    #, {'provider': 'Virgin Money',
    #  'gsheets_key': '1gIK0X_998BWh5OGMdR481hXsfwL4VCoo1eyAe1qeJUE'},
    # {'provider': 'NRMA',
    #  'gsheets_key': '1e6M4XxeSYbWhf-8ISjd3_IS24vyFD59omTtEkNKmQhQ'},


# note that the Sheets files must be shared with pygsheets@mozo-private-dev.iam.gserviceaccount.com

########################################
