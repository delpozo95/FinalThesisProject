import os
from os import path
import datetime
import shutil

directory = '/root/regs/'
daysToKeepCSV=3

creationDates=[]
dates=[]
past = datetime.datetime.now() - datetime.timedelta(days=daysToKeepCSV)
for dirpath, dirnames, filenames in os.walk(directory):
    for directoryName in dirnames:
        creationDate=directoryName.split('-')
        date=datetime.datetime(int(creationDate[0]), int(creationDate[1]), int(creationDate[2]))#.strftime("%Y-%m-%d")
        creationDates.append(creationDate)
        dates.append(date)
for date in dates:
    print(date)
    print(past)
    print(date<past)
    if((date<past)==True):
        print("to delete: ",date.strftime("%Y-%m-%d"))
        shutil.rmtree(str(directory)+date.strftime("%Y-%m-%d"))




