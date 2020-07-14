#!/usr/bin/python3
import pyodbc
import datetime
import re

def openDB(databaseHost,username,password,database_name):
    return pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      f'Server={databaseHost};'
                      f'Database={database_name};'
                      f'UID={username};'
                      f'PWD={password}')

def closeDB(dataBase):
    try:
        dataBase.close()
        return 1
    except pyodbc.Error as ex:
        logError="ERROR: "+str(ex.args[1])
        try:
            logFile=open(PATH_TO_ERRORS_DIR+"_ErrorLog.txt","+a")
            logFile.write(logError+"\n")
            logFile.close()
        except:
            print("Error opening that file")
        return 0

def tableCreation(dataBase,tableNameToBeCreated,HEADERS_ERRORS,TYPE_ERRORS,OPTIONS_ERRORS):
    listColumnNamesPlusOptions=[]#used to create the table
    for i in range(0,len(HEADERS_ERRORS)):
        listColumnNamesPlusOptions.append([HEADERS_ERRORS[i],TYPE_ERRORS[i]+OPTIONS_ERRORS[i]])
    cursor = dataBase.cursor()
    temporal = " "
    counter=1
    for pair in listColumnNamesPlusOptions:
        temporal+=str(pair[0])+" "+ str(pair[1])
        if(counter<len(listColumnNamesPlusOptions)):
            temporal+=", "
        else:
            temporal+=" );"
        counter+=1
    sql = "CREATE TABLE "+str(tableNameToBeCreated)+"( "+temporal
    try:
        cursor.execute(sql)
        dataBase.commit()
    except pyodbc.Error as ex:
        logError="ERROR: "+str(ex.args[1])+sql
        try:
            logFile=open(PATH_TO_ERRORS_DIR+"_ErrorLog.txt","+a")
            logFile.write(logError+"\n")
            logFile.close()
        except:
            print("Error opening that file")
    cursor.close()

def tableCreation(dataBase,TABLE_NAME_ERRORS,HEADERS_ERRORS,TYPE_ERRORS,OPTIONS_ERRORS):
    cursor = dataBase.cursor()
    sql = "CREATE TABLE "+TABLE_NAME_ERRORS+" "+prepareTableHeaders(HEADERS_ERRORS,TYPE_ERRORS,OPTIONS_ERRORS)
    try:
        cursor.execute(sql)
        dataBase.commit()
    except pyodbc.Error as ex:
        logError="ERROR: "+str(ex.args[1])+sql
        try:
            logFile=open(PATH_TO_ERRORS_DIR+"_ErrorLog.txt","+a")
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
            logFile=open(PATH_TO_ERRORS_DIR+"_ErrorLog.txt","+a")
            logFile.write(logError+"\n")
            logFile.close()
        except:
            print("Error opening that file")
    cursor.close()

def setRowsData(dataBase,tableName,columnNames,values):
    cursor = dataBase.cursor()
    sql = str("INSERT INTO "+str(tableName)+" "+columnNames+" VALUES "+values)
    try:
        cursor.execute(sql)
        dataBase.commit()
    except pyodbc.Error as ex:
        logError="ERROR: "+str(ex.args[1])+sql
        try:
            logFile=open(PATH_TO_ERRORS_DIR+"_ErrorLog.txt","+a")
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
            logFile=open(PATH_TO_ERRORS_DIR+"_ErrorLog.txt","+a")
            logFile.write(logError+"\n")
            logFile.close()
        except:
            print("Error opening that file")
    finally:
        cursor.close()


def getRowsData(dataBase, tableName,option=0):
    cursor = dataBase.cursor()
    if(option==0):
        sql = "SELECT DATE FROM "+str(tableName)+";"
    elif(option==1):
        sql = "SELECT * FROM "+str(tableName)+";"
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
    except pyodbc.Error as ex:
        logError="ERROR: "+str(ex.args[1])+sql
        result = None
        try:
            logFile=open(PATH_TO_ERRORS_DIR+"_ErrorLog.txt","+a")
            logFile.write(logError+"\n")
            logFile.close()
        except:
            print("Error opening that file")
    cursor.close()
    return result

def checkInverterFailures(host,user,password,databaseName,TABLENAME):
    db=openDB(host,user,password,databaseName)
    result=getRowsData(db,TABLENAME,option=0)
    yesterday = datetime.datetime.today() - datetime.timedelta(days=1) + timeCorrection
    count=0
    if result:
        for datee in result:
            if(datee[0]>yesterday):
                count+=1
    try:
        logFile=open(PATH_TO_COMM_DIR+"CommunicationLog.txt","+a")
        logFile.write("\n\nPercentage of data into database at "+datetime.date.today().strftime("%Y-%m-%d")+":  "+str(float(count*100/DAILY_NUMBER_OF_VALUES))+"%\n")
        logFile.close()
    except:
        print("Error opening that file")
    finally:
        logFile.close()
        closeDB(db)
        return float(count*100/DAILY_NUMBER_OF_VALUES)


def checkInverterFailures2(host,user,password,databaseName,TABLENAME):
    try:
        db=openDB(host,user,password,databaseName)
        result=getRowsData(db,TABLENAME,option=0)
        yesterday = datetime.datetime.today() - datetime.timedelta(days=1) + timeCorrection
        count=0
        if result:
            for datee in result:
                if(datee[0]>yesterday):
                    count+=1
    except:
        try:
            logFile=open(PATH_TO_ERRORS_DIR+"_ErrorLog.txt","+a")
            logFile.write(logError+"\n")
            logFile.close()
        except:
            print("Error opening that file")
        finally:
            return -1
    finally:
        closeDB(db)
        return float(count*100/DAILY_NUMBER_OF_VALUES)


    
def toStringCPH(opt,arr,schema=None,correctionChar=""):
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
                if(schema[i]=="DATE"):
                    s+=correctionChar+str(arr[j][i])+correctionChar+", "
                else:
                    s+=str(arr[j][i])+", "
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

def checkSQLandFTPFailures(host,user,password,databaseName,TABLENAME):
    percentage=checkInverterFailures2(host,user,password,databaseName,TABLE_NAME_INVERTERS)
    totalErrors=0
    PKErrors=0
    overflowErrors=0
    ftpErrors=0
    ftpErrors2=0
    errors=[]
    try:
        with open(PATH_TO_ERRORS_DIR+'_ErrorLog.txt', 'r') as myfile:
            fp2 = myfile.read()
        try:
            patternNumberOfErrors = re.compile(r'. \((\d)+\)')
            totalErrors=len(re.findall(patternNumberOfErrors,fp2))
        except:
            totalErrors=-1

        try:
            patternPrimaryKey = re.compile(r'. \(2627\)')
            PKErrors=len(re.findall(patternPrimaryKey,fp2))
        except:
            PKErrors=-1

        try:
            patternOverflow = re.compile(r'. \(220\)')
            overflowErrors=len(re.findall(patternOverflow,fp2))
        except:
            overflowErrors=-1

        try:
            patternFTPFailure = re.compile(r'. \(55555\)')
            ftpErrors=len(re.findall(patternFTPFailure,fp2))
        except:
            ftpErrors=-1

        try:
            patternFTPFailure2 = re.compile(r'. \(55554\)')
            ftpErrors2=len(re.findall(patternFTPFailure2,fp2))
        except:
            ftpErrors2=-1        

    except:
        print("There is not error today")
    finally:
        print("END")
        try:
            logFile=open(PATH_TO_COMM_DIR+"CommunicationLog.txt","+a")
            logFile.write("\n\nPercentage of data into database at "+datetime.date.today().strftime("%Y-%m-%d")+":  "+str(percentage)+"%\n")
            logFile.write("Number of SQL errors at "+datetime.date.today().strftime("%Y-%m-%d")+":  "+str(totalErrors)+"\n")
            if(totalErrors==0):
                logFile.write("      Percentage of SQL errors due to primary key repeated at "+datetime.date.today().strftime("%Y-%m-%d")+":  "+str(PKErrors*100/1)+"%\n")
                logFile.write("      Percentage ofSQL errors due to overflow at "+datetime.date.today().strftime("%Y-%m-%d")+":  "+str(overflowErrors*100/1)+"%\n")
            else:
                logFile.write("      Percentage of SQL errors due to primary key repeated at "+datetime.date.today().strftime("%Y-%m-%d")+":  "+str(PKErrors*100/totalErrors)+"%\n")
                logFile.write("      Percentage ofSQL errors due to overflow at "+datetime.date.today().strftime("%Y-%m-%d")+":  "+str(overflowErrors*100/totalErrors)+"%\n")
            logFile.write("      Number of SQL errors due to FTP server problems at "+datetime.date.today().strftime("%Y-%m-%d")+":  "+str(ftpErrors)+"\n")
            logFile.write("      Number of SQL errors due to not file at FTP server at "+datetime.date.today().strftime("%Y-%m-%d")+":  "+str(ftpErrors2)+"\n")
            logFile.write("      Number of SQL errors due to other reasons at "+datetime.date.today().strftime("%Y-%m-%d")+":  "+str(totalErrors-ftpErrors-PKErrors-overflowErrors-ftpErrors2)+"\n")
            logFile.close()
        except Exception as e:
            print("File not accessible and Error: ",e)
        finally:
            errors.append(datetime.date.today())
            errors.append(percentage)
            errors.append(PKErrors)
            errors.append(overflowErrors)
            errors.append(ftpErrors)
            errors.append(ftpErrors2)
            errors.append(totalErrors-ftpErrors-PKErrors-overflowErrors-ftpErrors2)
            return errors

# Credenciales de la BD y conexión al blob

user        = 'user'
password    = 'password'
host = 'host'
databaseName='databaseName'

PATH_TO_CSV_FILES='path/files/'+str(datetime.date.today())+'/' #to the CSV files
PATH_TO_ERRORS_DIR="path/ErrorLogs/"
PATH_TO_COMM_DIR="path/Communications/"

hoursCorrection=0
timeCorrection=datetime.timedelta(hours=hoursCorrection)
DAILY_NUMBER_OF_VALUES=90*4

HEADERS_ERRORS=["DATE","PERCENTAGE","N2627","N220","N55555","N55554","N55556"] #name of colummns
TYPE_ERRORS=["DATE","FLOAT","INT","INT","INT","INT","INT"] #value of columns
OPTIONS_ERRORS=["PRIMARY KEY","NOT NULL","NOT NULL","NOT NULL","NOT NULL","NOT NULL","NOT NULL"] #value of columns
TABLE_NAME_ERRORS='ERRORLOG'
TABLE_NAME_INVERTERS='DAQIN'

errors=checkSQLandFTPFailures(host,user,password,databaseName,TABLE_NAME_INVERTERS)
db=openDB(host,user,password,databaseName)
tableErased(db,TABLE_NAME_ERRORS)
tableCreation(db,TABLE_NAME_ERRORS,HEADERS_ERRORS,TYPE_ERRORS,OPTIONS_ERRORS)
setRowsData(db,TABLE_NAME_ERRORS,HEADERS_ERRORS,TYPE_ERRORS,[errors],correctionChar="\'")
closeDB(db)
print("end of set")


