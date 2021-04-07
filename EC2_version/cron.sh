#!/bin/bash
SHELL=/bin/sh
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin

#DATEVAR=date +%Y_%m_%d
00 21 * * 1-5 cd ~/financials-downloader-bot && python3 run.py -e "<>" -p "<>" > ~/financials-downloader-bot/crontab.log 2>&1

10 21 * * 1-5 . ~/.bashrc; cd ~/Signal-Detection && python3 transferTechnicals.py
11 21 * * 1-5 . ~/.bashrc; cd ~/Signal-Detection && python3 transferOwnership.py


30 9 * * 1 cd ~/financials-downloader-bot && :> geckodriver.log

# Get EOD (Tueseday to Saturday, because end US market = 22 here and EOD data gets uploaded after 24)
8 7 * * 2-6 cd ~/eoddata && python3 eoddata.py -e '<>' -p '<>' -s 'NYSE' > downloads/logs/log_$(date +\%Y_\%m_\%d).log
9 7 * * 2-6 cd ~/eoddata && python3 eoddata.py -e '<>' -p '<>' -s 'NASDAQ' >> downloads/logs/log_$(date +\%Y_\%m_\%d).log

# Get env. variables & push eod data to RDS
10 7 * * 2-6 . ~/.bashrc; cd ~/eoddata && python3 dataTransfer.py >> downloads/logs/log_$(date +\%Y_\%m_\%d).log

16 7 * * 2-6 . ~/.bashrc; cd ~/Signal-Detection && python3 Signal_detection.py

30 7 * * 2-6 . ~/.bashrc; cd ~/Signal-Detection && python3 signalsEvol.py

33 7 * * 2-6 . ~/.bashrc; cd ~/Signal-Detection && python3 DetailedGeneration.py

25 21 * * 1-5 . ~/.bashrc; cd ~/Signal-Detection && python3 sp500.py

