import pyodbc
import random
import datetime
import csv

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def openDB(databaseHost,username,password,database_name):
    return pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      f'Server={databaseHost};'
                      f'Database={database_name};'
                      f'UID={user};'
                      f'PWD={password}')

def closeDB(dataBase):
    try:
        dataBase.close()
        return 1
    except pyodbc.Error as ex:
        logError="ERROR: "+str(ex.args[1])
        print(logError)
        try:
            logFile=open(PATH_TO_ERRORS_DIR+datetime.date.today().strftime("%Y-%m-%d")+"_ErrorLog.txt","+a")
            logFile.write(logError+"\n")
            logFile.close()
        except:
            print("Error opening that file")
        return 0

def getRowsDataToBePainted(dataBase, tableName, xName='*', yName='', endDate=datetime.date.today().strftime("%Y-%m-%d"), DaysBefore=1, extraClause=''):
    cursor = dataBase.cursor()
    result = None
    sql = "SELECT "+str(xName)+', '+str(yName)+" FROM "+str(tableName)+" WHERE "+'\''+str(endDate)+'\''+" BETWEEN GETDATE() -"+str(DaysBefore)+" AND GETDATE() "+str(extraClause)+";"
    try:
        print(sql)
        cursor.execute(sql)
        result = cursor.fetchall()
    except pyodbc.Error as ex:
        logError="ERROR: "+str(ex.args[1])+sql
        print(logError)
    cursor.close()
    return result



TABLE_NAME_INVERTER='DAQIN'
MAGNITUDE='PAC1'
INVERTER=2

user        = 'user'
password    = 'password'
host = 'host'
databaseName='databaseName'

data=[[],[]]

db=openDB(host,user,password,databaseName)
print("connected")
result=getRowsDataToBePainted(db, TABLE_NAME_INVERTER, 'DATE', MAGNITUDE, datetime.date.today().strftime("%Y-%m-%d"), 1, extraClause='AND INV='+str(INVERTER))
closeDB(db)
print("closed")
if(result):#[(x,y),(x,y)]
    print(result)
    for i in range (0,len(result)):#(x,y)
        for j in range (0,len(result[i])):
            #data[j].append((result[i][j]).strftime("%X"))
            data[j].append((result[i][j]))

x = mdates.date2num(data[0]) 
formatter = mdates.DateFormatter('%H:%M') 
figure = plt.figure() 
axes = figure.add_subplot(1, 1, 1) 
axes.set(xlabel="Date",
       ylabel="IDC (A)",
       title="Daily Total IDC")
axes.xaxis.set_major_formatter(formatter) 
plt.setp(axes.get_xticklabels(), rotation = 15) 
axes.plot(x, data[1]) 
plt.show() 

print("END")