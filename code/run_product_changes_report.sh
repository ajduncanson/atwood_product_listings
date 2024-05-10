#!/bin/bash

cd ~/projects/sql_to_gsheets/code

start_time=$(TZ=Australia/Sydney date '+%Y-%m-%d %H:%M:%S')
log_file='../logs/started_'${start_time:0:10}'.log'
echo '\n'$start_time'\tStart' >> $log_file

echo 'Running product changes report now' >> $log_file
~/.pyenv/versions/cdr-project-env/bin/python3 product_changes_report.py >> $log_file 2>&1

if [[ $(find ../data/product_changes_report/success.txt -mmin -5 -print) ]]
then
  echo 'Report success' >> $log_file
  mail -s "Product changes report OK" analytics@mozo.com.au <<< "Product changes report ran sucessfully."  
else
  echo 'Report error' >> $log_file
  mail -s "Failed: Product changes report" analytics@mozo.com.au <<< "Product changes report error."
fi

cd /
