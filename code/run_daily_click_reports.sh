#!/bin/bash

cd ~/projects/sql_to_gsheets/code

start_time=$(TZ=Australia/Sydney date '+%Y-%m-%d %H:%M:%S')
log_file='../logs/started_'${start_time:0:10}'.log'
echo '\n'$start_time'\tStart' >> $log_file

echo 'Running daily provider click reports now' >> $log_file
~/.pyenv/versions/cdr-project-env/bin/python3 daily_click_reports.py >> $log_file 2>&1

if [[ $(find ../data/daily_click_reports/success.txt -mmin -5 -print) ]]
then
  echo 'Report success' >> $log_file
  mail -s "Daily Provider Click Reports" analytics@mozo.com.au <<< "Daily provider click reports ran sucessfully."  
else
  echo 'Report error' >> $log_file
  mail -s "Failed: Daily Provider Click Reports" analytics@mozo.com.au <<< "Daily provider click reports error."
fi

cd /
