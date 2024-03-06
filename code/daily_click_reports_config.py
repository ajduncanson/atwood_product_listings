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
    
    # this one drives a simple totals report for Neville
    {'provider': 'All',
     'content': '',
     'gsheets_key': '1xznF3FANA1o2BK7U_XvhYKjaAjUWbTV0O21XV3rL7CI'
    },

    # this little piggy has special grouping logic
    {'provider': 'Lendi',
     'content': {'HomeLoanBrokerReferral': {'cols': ['Clicks', 'Spend'], 'product_level': 'regex'}
                 },
     'gsheets_key': '1fMYGVXwxUKmbvW6gVx50l7cvSNPtW17CEb1h6S0MGU0',
     'grouping': {'regex': 'TTAB', 
                  'label': 'Talk To A Broker', 
                  'other_label': 'Other placements'}
     },

    {'provider': 'Harmoney',
     'content': {'PersonalLoan': {'cols': ['Clicks', 'Spend'], 'product_level': 'product'}
                 },
     'gsheets_key': '1QVjVDXhTx-9No8WR3gItG_GpVKvmbD35AcMdryjyANo'},

    {'provider': 'Ubank',
     'content': {'HomeLoan': {'cols': ['Clicks', 'Spend'], 'product_level': 'type'},
                 'SavingsAccount': {'cols': ['Clicks', 'Spend'], 'product_level': 'type'},
                 'BankAccount': {'cols': ['Clicks', 'Spend'], 'product_level': 'type'}
                 },
     'gsheets_key': '1f4qC3I9-TWnFA41ZjslWtYw45COX_Oim5MABYRKrjNo'},

    # {'provider': 'Bendigo Bank',
    #  'content': {'HomeLoan': {'cols': ['Clicks', 'Apps', 'Spend'], 'product_level': 'type'}
    #              },
    #  'gsheets_key': '1Lw-p5ocXlLTw8gv9Nw0gXhWq2k_OhWCY6qoTtVjWJo0'},

    {'provider': 'Bendigo Bank',
     'content': {'BankAccount': {'cols': ['Clicks', 'Spend'], 'product_level': 'type'},
                 'SavingsAccount': {'cols': ['Clicks', 'Spend'], 'product_level': 'type'}
                 },
     'gsheets_key': '19xiTo-f5z-IVLRKQwFuQkm39TMr6x5nt_wnvCUOS4Qo'},

    {'provider': 'OurMoneyMarket',
     'content': {'PersonalLoan': {'cols': ['Clicks', 'Spend'], 'product_level': 'type'},
                 'CarLoan': {'cols': ['Clicks', 'Spend'], 'product_level': 'type'},
                 },
     'gsheets_key': '1IIqNKIm7J9izhmSHWlMKWq06rWC-84rXULoJgb1YTWg'},

     {'provider': 'honey',
     'content': {'HomeInsurance': {'cols': ['Clicks'], 'product_level': 'type'}
                 },
     'gsheets_key': '1nhz0sp-y3tp8caPJB8CF-RBdC1iAmJCAHe5Af9sxe_s'},

     {'provider': 'AMP Bank',
     'content': {'BankAccount': {'cols': ['Clicks', 'Spend'], 'product_level': 'type'}
                 },
     'gsheets_key': '1zcWeCHy8q8EwcAb492pEsJNL8cSoVyxjbsd-63hjB08'},

     {'provider': 'Freely',
     'content': {'TravelInsurance': {'cols': ['Clicks'], 'product_level': 'type'}
                 },
     'gsheets_key': '1rL_8mIOvqAjGrEaQFbiWCxFoikhdQZGQHxJ0d5dca0g'},

     {'provider': 'Virgin Money',
     'content': {'CarInsurance': {'cols': ['Clicks'], 'product_level': 'type'},
                 'HomeInsurance': {'cols': ['Clicks'], 'product_level': 'type'}
                 },
     'gsheets_key': '1gIK0X_998BWh5OGMdR481hXsfwL4VCoo1eyAe1qeJUE'},

     {'provider': 'Rabobank',
     'content': {'SavingsAccount': {'cols': ['Clicks', 'Spend'], 'product_level': 'product'},
                 'TermDeposit': {'cols': ['Clicks', 'Spend'], 'product_level': 'product'}
                 },
     'gsheets_key': '1QOu93cI8mZ8LvjHpGQ96Hj2KVOPiLkSbXktpnqCQDEE'},

     {'provider': 'Zoom',
     'content': {'TravelInsurance': {'cols': ['Clicks'], 'product_level': 'type'}
                 },
     'gsheets_key': '1-5wI3IAOeioAFNSLw6Be9nEUSJW2uJ_jhfwqew2clh8'},

     {'provider': 'Macquarie',
     'content': {'HomeLoan': {'cols': ['Clicks', 'Spend'], 'product_level': 'type'},
                 'SavingsAccount': {'cols': ['Clicks', 'Spend'], 'product_level': 'type'}
                 },
     'gsheets_key': '1yxNSPpK33iUjxBjzOR2UF77h7Cj-K3uOSx2DZO2QvOA'},

     {'provider': 'NRMA',
     'content': {'CarLoan': {'cols': ['Clicks', 'Spend'], 'product_level': 'type'}
                 },
     'gsheets_key': '1e6M4XxeSYbWhf-8ISjd3_IS24vyFD59omTtEkNKmQhQ'},

     {'provider': 'Judo Bank',
     'content': {'TermDeposit': {'cols': ['Clicks', 'Spend'], 'product_level': 'type'}
                 },
     'gsheets_key': '1I8tWPmWyqhsYU7o5Htd8UDgSqwL4mF77H85DZg94rFo'},

    {'provider': 'Sucasa',
     'content': {'HomeLoan': {'cols': ['Clicks', 'Spend'], 'product_level': 'type'}
                 },
     'gsheets_key': '1gpmI8quI-JaRSHJ9aWNRa__c9tM1P4wCK6xL19r9Buw'},

    {'provider': 'Athena',
     'content': {'HomeLoan': {'cols': ['Clicks', 'Spend'], 'product_level': 'type'}
                 },
     'gsheets_key': '1mTTOBit65I9Vk9-6n4V58iIRbcjpO4S5NbsLg-wf0Rs'}

]  


# note that the Sheets files must be shared with pygsheets@mozo-private-dev.iam.gserviceaccount.com

########################################
