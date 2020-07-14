#!/usr/bin/python3
from ftplib import FTP_TLS
import os
from os import path
import datetime
import shutil
import paho.mqtt.client as mqtt

def deleteOldFiles():
    try:
        creationDates=[]
        dates=[]
        past = datetime.datetime.now() - datetime.timedelta(days=daysToKeepCSV)
        for dirpath, dirnames, filenames in os.walk(directoryToDelete):
            for directoryName in dirnames:
                creationDate=directoryName.split('-')
                date=datetime.datetime(int(creationDate[0]), int(creationDate[1]), int(creationDate[2]))#.strftime("%Y-%m-%d")
                creationDates.append(creationDate)
                dates.append(date)
        for date in dates:
            if((date<past)==True):
                shutil.rmtree(str(directory)+date.strftime("%Y-%m-%d"))
    except:
        #print("Error: " ,e)
        client1.connect(broker,port)
        ret= client1.publish("/upm/qpvies/ftplog/deletion","Error to delete old files in VM at: "+ (datetime.datetime.now()+timeCorrection).strftime("%Y-%m-%d %H:%M:%S"), qos=0, retain=True)



broker = 'broker'
port = 1883
hoursCorrection=0
timeCorrection=datetime.timedelta(hours=hoursCorrection)
foldername=str(datetime.date.today())
PATH_TO_SAVE_CVSDIRECTORIES = '/path/files/'
directoryToDelete = '/path/files/'
daysToKeepCSV=1


client1= mqtt.Client() 
deleteOldFiles()

try:
    ftps = FTP_TLS('host')                                                                                                                                                                                               
    ftps.login('user', 'password') 
    ftps.prot_p()          # switch to secure data connection   
    ftps.cwd('dir')
    ftps.cwd(str(datetime.date.today()))
    files = ftps.nlst() # Get All Files
    for file in files:
        #print("Downloading..." + file)
        if(os.path.isdir(PATH_TO_SAVE_CVSDIRECTORIES +str(datetime.date.today()))==False):
            os.mkdir(PATH_TO_SAVE_CVSDIRECTORIES +str(datetime.date.today()))
        ftps.retrbinary("RETR " + file ,open(str(PATH_TO_SAVE_CVSDIRECTORIES +str(datetime.date.today())+"/"+ file), 'wb').write)
    ftps.close()
    #print("END OK")

except:
    client1.connect(broker,port)                                                                                        
    ret= client1.publish("/topic","Error to connect to FTP server from VM at: "+ (datetime.datetime.now()+timeCorrection).strftime("%Y-%m-%d %H:%M:%S"), qos=0, retain=True) 

#print("END")
