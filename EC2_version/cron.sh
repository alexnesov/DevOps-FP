#!/bin/bash
SHELL=/bin/sh
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin

#DATEVAR=date +%Y_%m_%d
00 21 * * 1-5 cd ~/financials-downloader-bot && python3 run.py -e "<email>" -p "<pass>" > ~/financials-downloader-bot/crontab.log 2>&1 && mail -s "Financials_$(date +\%Y_\%m_\%d)" alexandrenesovic@gmail.com < crontab.log
30 9 * * 1 cd ~/financials-downloader-bot && :> geckodriver.log


# Get EOD (Tueseday to Saturday, because end US market = 22 here and EOD data gets uploaded after 24)
54 10 * * 2-6 cd ~/eoddata && python3 eoddata.py -e '<email>' -p '<pass>' -s 'NYSE' > log_$(date +\%Y_\%m_\%d).log
55 10 * * 2-6 cd ~/eoddata && python3 eoddata.py -e '<email>' -p '<pass>' -s 'NASDAQ' >> log_$(date +\%Y_\%m_\%d).log

# TEST - Get env. variables
# 35 11 * * 2-6 . ~/.bashrc; cd ~/eoddata && echo ${aws_db_user} >> testing.log


# Get env. variables & push eod data to RDS
10 11 * * 2-6 . ~/.bashrc; cd ~/eoddata && python3 dataTransfer.py >> log_$(date +\%Y_\%m_\%d).log

