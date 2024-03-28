#!/bin/bash

cd ~/projects/sql_to_gsheets/code

start_time=$(TZ=Australia/Sydney date '+%Y-%m-%d %H:%M:%S')
log_file='../logs/started_'${start_time:0:10}'.log'
echo '\n'$start_time'\tStart' >> $log_file

echo 'Running atwood product listing reports now' >> $log_file
~/.pyenv/versions/cdr-project-env/bin/python3 atwood_product_listings_report.py >> $log_file 2>&1

if [[ $(find ../data/atwood_products/success.txt -mmin -5 -print) ]]
then
  echo 'Report success' >> $log_file
  mail -s "Atwood product listings report" analytics@mozo.com.au <<< "Atwood report ran sucessfully."  
else
  echo 'Report error' >> $log_file
  mail -s "Failed: Atwood products report" analytics@mozo.com.au <<< "Atwood product listings report error."
fi

cd /
