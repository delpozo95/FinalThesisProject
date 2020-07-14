#!/usr/bin/python3
from ftplib import FTP_TLS
import datetime
from io import StringIO
import paho.mqtt.client as mqtt
broker = 'IP'
port = 1883
hoursCorrection=2
timeCorrection=datetime.timedelta(hours=hoursCorrection)

#global variables
CSV1 = '_Inverter1.csv'
CSV2 = '_Inverter2.csv'
CSV3 = '_Inverter3.csv'
CSV4 = '_Inverter4.csv'
CSV = [CSV1,CSV2,CSV3,CSV4]
foldername=str(datetime.date.today())
PATH_TO_SAVE_CVSDIRECTORIES = '/root/regs/'

client1= mqtt.Client() 

try:
    ftps = FTP_TLS('host')                                                                                                                                                                                               
    ftps.login('user', 'password')           # login anonymously before securing control channel                                                                                                                                         
    ftps.prot_p()          # switch to secure data connection   
    ftps.cwd('dir') # execute a cd command to access to directory                                                  
    if (not(foldername in ftps.nlst())):                                                                                     
        ftps.mkd(foldername) #create a new folder with current date                                                      
    ftps.cwd(str(datetime.date.today())) #access to current date folder   

    for i in range(0,len(CSV)):
        filename=str(PATH_TO_SAVE_CVSDIRECTORIES+str(datetime.date.today())+'/'+str(datetime.date.today())+CSV[i])
        file=open(filename,'rb')
        filename2=str(datetime.date.today())+CSV[i]
        ftps.storbinary('STOR '+filename2,file)
        file.close()
    ftps.quit()  
    print("End try")

except:
    print("except")
    client1.connect(broker,port)                                                                                        
    ret= client1.publish("/topic","Error to connect to FTP server at: "+ (datetime.datetime.now()+timeCorrection).strftime("%Y-%m-%d %H:%M:%S"), qos=0, retain=True) 

print("END")
