#!/usr/bin/python3
import pyodbc
import random
import datetime
import csv

#dataExample=[[["1","2"],["3","4"]], [["1","2"],["3","4"]]]
#schemaToDeleteExample=[4,3,1],[6, 3] delete the values 1 3 and 4 in the first file and 6 and 3 in the next one It is very important to send in inverter order.
#dataToAddExample=[["1","1"],["1","2"],["1","3"],["2","4"]]
def standardizeDataForSQL(data,schemaToDelete,dataToAdd):
    dataStartardized=data
    for deviceFile in range(0,len(data)): #separate each device data example: [["1","2"],["3","4"]]
        for lineOfData in range (0,len(data[deviceFile])): #separate each row of data ["1","2"]
            for index in schemaToDelete[deviceFile]:
                dataStartardized[deviceFile][lineOfData].pop(index)#remove all data that is not useful for SQL database
            for toAdd in dataToAdd[deviceFile]:
                dataStartardized[deviceFile][lineOfData].append(toAdd)#add the extra data
    return dataStartardized #return [[["1","2"],["3","4"]], [["1","2"],["3","4"]]] but standardized to the SQL table of the  database

def joinInSameList(data,division):
    toReturn=[]
    mono=[]
    tri=[]
    for i in range(0,len(data)): #separate each device data example: [["1","2"],["3","4"]]
        for lineOfData in data[i]: #separate each row of data ["1","2"]
            if(i<division):
                mono.append(lineOfData)
            else:
                tri.append(lineOfData)
    toReturn.append(mono)
    toReturn.append(tri)
    return toReturn


def readCSVFilesByName(path, names, delimiter, rowStart=1, rowEnd=0, columnStart=0, columnEnd=0):
    data=[]
    for name in names:
        data.append(readCSV(path+name, delimiter, rowStart, rowEnd, columnStart, columnEnd))
    return data #return [[["1","2"],["3","4"]], [["1","2"],["3","4"]]] when [] means whole->[file->[dataPerRow->[

def readCSV(fileName, delimiter, rowStart=1, rowEnd=0, columnStart=0, columnEnd=0):
    rows=[]
    try:
        with open (fileName, mode='r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=delimiter)
            try:
                line=0
                for row in csv_reader:
                    line+=1
                    temp=[]
                    if(line>=rowStart):
                        if(len(row)>2):#to avoid white lines
                            for i in range(columnStart,columnEnd):
                                try:
                                    temp.append(row[i])
                                except Exception as e:
                                    break
                            rows.append(temp)
                    if(line>=rowEnd)and(rowEnd!=0):
                        break
                return rows
            except csv.Error as e:
                print("Error"+str(e)) 
    except:
        try:
            logFile=open(PATH_TO_ERRORS_DIR+datetime.date.today().strftime("%Y-%m-%d")+"_ErrorLog.txt","+a")
            logFile.write("ERROR. (55555) Error accessing to FTP server files at "+(datetime.datetime.now()+timeCorrection).strftime("%Y-%m-%d %H:%M:%S") +"\n")
            logFile.close()
        except:
            print("Error opening that file") 

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
        try:
            logFile=open(PATH_TO_ERRORS_DIR+datetime.date.today().strftime("%Y-%m-%d")+"_ErrorLog.txt","+a")
            logFile.write(logError+"\n")
            logFile.close()
        except:
            print("Error opening that file")
        return 0

def tableCreation(dataBase,TABLE_NAME_ERRORS,HEADERS_ERRORS,TYPE_ERRORS,OPTIONS_ERRORS):
    cursor = dataBase.cursor()
    sql = "CREATE TABLE "+TABLE_NAME_ERRORS+" "+prepareTableHeaders(HEADERS_ERRORS,TYPE_ERRORS,OPTIONS_ERRORS)
    try:
        ##print(sql)
        cursor.execute(sql)
        dataBase.commit()
    except pyodbc.Error as ex:
        logError="ERROR: "+str(ex.args[1])+sql
        try:
            logFile=open(PATH_TO_ERRORS_DIR+datetime.date.today().strftime("%Y-%m-%d")+"_ErrorLog.txt","+a")
            logFile.write(logError+"\n")
            logFile.close()
        except:
            print("Error opening that file")
    finally:
        cursor.close()

def tableErased(dataBase,tableNameToBeErased):
    cursor = dataBase.cursor()
    try:
        sql = "DROP TABLE "+str(tableNameToBeErased)
        cursor.execute(sql)
        dataBase.commit()
    except pyodbc.Error as ex:
        logError="ERROR: "+str(ex.args[1])+sql
        try:
            logFile=open(PATH_TO_ERRORS_DIR+datetime.date.today().strftime("%Y-%m-%d")+"_ErrorLog.txt","+a")
            logFile.write(logError+"\n")
            logFile.close()
        except:
            print("Error opening that file")
    cursor.close()

def setRowsData(dataBase,TABLE_NAME,HEADERS,SCHEMA,values,correctionChar="\'"):
    cursor = dataBase.cursor()
    columnNames=toStringCPH(False,HEADERS)
    stringValuesArray=toStringCPH(True,values,SCHEMA,correctionChar)
    try:
        for stringValues in stringValuesArray: 
            sql = str("INSERT INTO "+str(TABLE_NAME)+" "+columnNames+" VALUES "+stringValues)
            cursor.execute(sql)
            dataBase.commit()
    except pyodbc.Error as ex:
        logError="ERROR: "+str(ex.args[1])+sql
        try:
            logFile=open(PATH_TO_ERRORS_DIR+datetime.date.today().strftime("%Y-%m-%d")+"_ErrorLog.txt","+a")
            logFile.write(logError+"\n")
            logFile.close()
        except:
            print("Error opening that file")
    finally:
        cursor.close()

def toStringCPH(opt,arr,schema=None,correctionChar=""):
    #t="\'"
    if(opt==False):
        s="("
        for i in range(0,(len(arr)-1)):
            s+=correctionChar+str(arr[i])+correctionChar+", "
        s+=str(arr[len(arr)-1])+")"
        return s
    elif(opt==True):
        sArr=[]
        for j in range(0,len(arr)):
            s="("
            for i in range(0,(len(arr[j])-1)):
                if(schema[i]=="DATETIME"):
                    s+=correctionChar+str(arr[j][i])+correctionChar+", "
                else:
                    s+=str(arr[j][i])+", "
            if(schema[i]=="DATETIME"):
                s+=correctionChar+str(arr[j][len(arr[j])-1])+correctionChar+")"
            else:
                s+=str(arr[j][len(arr[j])-1])+")"
            sArr.append(s) 
        return sArr

def prepareTableHeaders(HEADERS_ERRORS,TYPE_ERRORS,OPTIONS_ERRORS):
    s="("
    for i in range(0,(len(HEADERS_ERRORS)-1)):
        s+=str(HEADERS_ERRORS[i])+" "+str(TYPE_ERRORS[i])+" "+str(OPTIONS_ERRORS[i])+", "
    s+=str(HEADERS_ERRORS[len(HEADERS_ERRORS)-1])+" "+str(TYPE_ERRORS[len(HEADERS_ERRORS)-1])+" "+str(OPTIONS_ERRORS[len(HEADERS_ERRORS)-1])+" )"
    return s

def createTableColumnNames(HEADERS_ERRORS,TYPE_ERRORS,OPTIONS_ERRORS):
    listColumnNamesPlusOptions=[]#used to create the table
    for i in range(0,len(HEADERS_ERRORS)):
        listColumnNamesPlusOptions.append([HEADERS_ERRORS[i],TYPE_ERRORS[i]+OPTIONS_ERRORS[i]])
    return listColumnNamesPlusOptions

##################################################################################################################################################################################################################
PATH_TO_CSV_FILES='path/files/'+str(datetime.date.today())+'/' #to the CSV files
PATH_TO_ERRORS_DIR="path/ErrorLogs/"

CSV1 = str(datetime.date.today())+'_Inverter1.csv'
CSV2 = str(datetime.date.today())+'_Inverter2.csv'
CSV3 = str(datetime.date.today())+'_Inverter3.csv'
CSV4 = str(datetime.date.today())+'_Inverter4.csv'
CSV = [CSV1,CSV2,CSV3,CSV4]

ADD=[["1","1"],["1","2"],["1","3"],["2","4"]]
DELETE=[[9,5,2,1,0],[9,5,2,1,0],[12,2,1,0],[12,2,1,0]]

HEADERS_INVERTERS=["DATE","CT","INV","VDC","IDC","PDC","VAC1","VAC2","VAC3","IAC1","IAC2","IAC3","PAC","PAC1","PAC2","PAC3","Q","Q1","Q2","Q3","S","S1","S2","S3","PF","COSFI","TEMP","FREQ","EAC"] #name of colummns
TYPE_INVERTERS=["DATETIME","INT","INT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT"] #value of columns
OPTIONS_INVERTERS=["PRIMARY KEY","NOT NULL","NOT NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL"] #value of columns
TABLE_NAME_INVERTER='DAQIN'

HEADERS_INVERTERS_MONO=["VDC","IAC1","IDC", "PAC1", "COSFI", "VAC1", "FREQ","DATE", "CT", "INV"] #name of colummns
TYPE_INVERTERS_MONO=["FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","DATETIME","INT", "INT"] #value of columns

HEADERS_INVERTERS_TRI=["VDC","IDC","VAC1","VAC2","VAC3","IAC1","IAC2","IAC3","COSFI","PDC","FREQ","PAC","DATE","CT","INV",] #name of colummns
TYPE_INVERTERS_TRI=["FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT","DATETIME","INT","INT"] #value of columns

user        = 'user'
password    = 'password'
host = 'host'
databaseName='databaseName'

##################################################################################################################################################################################################################

datos=[]
datos=readCSVFilesByName(PATH_TO_CSV_FILES, CSV,',',2,0,0,20)
datos=standardizeDataForSQL(datos,DELETE,ADD)
datos=joinInSameList(datos,2)
db=openDB(host,user,password,databaseName)
tableErased(db,TABLE_NAME_INVERTER)
tableCreation(db,TABLE_NAME_INVERTER,HEADERS_INVERTERS,TYPE_INVERTERS,OPTIONS_INVERTERS)
setRowsData(db,TABLE_NAME_INVERTER,HEADERS_INVERTERS_MONO,TYPE_INVERTERS_MONO,datos[0],correctionChar="\'")
setRowsData(db,TABLE_NAME_INVERTER,HEADERS_INVERTERS_TRI,TYPE_INVERTERS_TRI,datos[1],correctionChar="\'")
closeDB(db)
print("end of set")



