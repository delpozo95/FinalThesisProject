#!/usr/bin/python3
from pyModbusTCP.client import ModbusClient
import csv
import time
import os.path
from os import path
import json
import datetime
import ast
hoursCorrection=2
timeCorrection=datetime.timedelta(hours=hoursCorrection)

# Funciones que leen n registros
def Read_N_Input_Registers(quantity, initReg = 0, client=None, addTimestamp=False, CustomSeconds=None):
    total = []
    parcial=[]
    try:
        for i in range(1,6,2):
            temp=client.read_input_registers(i, 1)
            total.append(temp[0])
        parcial = client.read_input_registers(initReg, quantity)
        total += parcial
        if (initReg==8):
            temp=client.read_input_registers(113, 1)
            total.append(temp[0]*10)
        if(addTimestamp):
            if(CustomSeconds!=None):
                tempo=str((datetime.datetime.now()+timeCorrection).replace(second=CustomSeconds).strftime("%Y-%m-%d %H:%M:%S") )
            else:
                tempo=str((datetime.datetime.now()+timeCorrection).strftime("%Y-%m-%d %H:%M:%S") )
            total.append(tempo)
        return total
    except Exception as e:
        print("Error"+str(e))

#Option==True if is monophasic
def formattingRegs(regs,option):
    data={}
    if(option==True):#MONOPHASIC
        try:
            regs[4]=float(float(regs[4])/100)
            regs[6]=float(float(regs[6])/100)
            regs[8]=float(float(regs[8])/1000)
            if(regs[9]>2):
                regs[9]='-'
            else:
                regs[9]='+'
            regs[11]=float(float(regs[11])/100)
            #regs.append(str((datetime.datetime.now()+timeCorrection).strftime("%Y-%m-%d %H:%M:%S") ))            
            #print(regs)
            for i in range(0,len(regs)):
                data[HEADERS_MONO[i]]=str(regs[i])
            json_data = json.dumps([data])
            return json_data
        except Exception as e:
            print("Error"+str(e))
    else:#TRIPHASIC
        try:
            regs[11]=float(float(regs[11])/1000)
            if(regs[12]>2):
                regs[12]='-'
            else:
                regs[12]='+'
            regs[13]=float(float(regs[13])*10)
            regs[14]=float(float(regs[14])/100)
            #regs.append(str((datetime.datetime.now()+timeCorrection).strftime("%Y-%m-%d %H:%M:%S") ))
            #print(regs)
            for i in range(0,len(regs)):
                data[HEADERS_TRI[i]]=str(regs[i])
            json_data = json.dumps([data])
            return json_data
        except Exception as e:
            print("Error"+str(e))
    
def checkDir(path):
    if(not(os.path.isdir(path+str(datetime.date.today())))):
        try:
            os.mkdir(path+str(datetime.date.today()))
        except OSError:
            print ("Creation of the directory %s failed")

def writeCSV(fileName,columnsNames,data,headers=False):
    head=headers
    if (path.exists(fileName)==False):
        print("No existe")
        head=True
    with open (fileName, mode='a', newline='') as csv_file:
        try:
            writer = csv.DictWriter(csv_file, fieldnames=columnsNames)
            if (head):
                writer.writeheader()
            for i in data:
                writer.writerow(i)
        except csv.Error as e:
            print("Error"+str(e))
        except Exception as e:
            print("Error"+str(e))

# Datos del cliente modbus
IP_MONOFACIALES = 'X.X.X.X'
IP_BIFACIALES = 'X.X.X.X'
CSV1 = '_Inverter1.csv'
CSV2 = '_Inverter2.csv'
CSV3 = '_Inverter3.csv'
CSV4 = '_Inverter4.csv'
CSV = [CSV1,CSV2,CSV3,CSV4]
PORT   = 502
DEV_ID1 = 1
DEV_ID2 = 2
DEV_ID3 = 3
DEV_ID4 = 4
DEV_ID = [DEV_ID1,DEV_ID2,DEV_ID3,DEV_ID4]
HEADERS_MONO=['TEI(kW/h)','TOT(h)','TNGC','Vpv(V)','Iac(A)','Vbus(V)','Ipv(A)','Pac(W)','Cos Phi','Sin Phi Sign','Vac(V)','Freq(Hz)','Date']
HEADERS_TRI=['TEI(kW/h)','TOT(h)','TNGC','Vdc(V)','Idc(A)','Vac1(V)','Vac2(V)','Vac3(V)','Iac1(A)','Iac2(A)','Iac3(A)','Cos Phi','Sin Phi Sign','Pdc(W)','Freq(Hz)','Pac(W)','Date']
# Registros
INIT_REG_MONO = 12
N_REGS_MONO = 9
INIT_REG_TRI = 8
N_REGS_TRI = 12
PATH_TO_SAVE_CVSDIRECTORIES = '/root/regs/'

#--------------INVERTER 1--------------#
client1 = ModbusClient(host = IP_MONOFACIALES, port = PORT, unit_id = DEV_ID[0], auto_open = True)
values = Read_N_Input_Registers(N_REGS_MONO, INIT_REG_MONO, client1, addTimestamp=True, CustomSeconds=DEV_ID[0]*10)
dataFormatted=formattingRegs(values,True)
checkDir(PATH_TO_SAVE_CVSDIRECTORIES)
writeCSV(PATH_TO_SAVE_CVSDIRECTORIES+str(datetime.date.today())+'/'+str(datetime.date.today())+CSV1,HEADERS_MONO,ast.literal_eval(dataFormatted))
client1.close()
#--------------INVERTER 2--------------#
client2 = ModbusClient(host = IP_MONOFACIALES, port = PORT, unit_id = DEV_ID[1], auto_open = True)
values = Read_N_Input_Registers(N_REGS_MONO, INIT_REG_MONO, client2, addTimestamp=True, CustomSeconds=DEV_ID[1]*10)
dataFormatted=formattingRegs(values,True)
checkDir(PATH_TO_SAVE_CVSDIRECTORIES)
writeCSV(PATH_TO_SAVE_CVSDIRECTORIES+str(datetime.date.today())+'/'+str(datetime.date.today())+CSV2,HEADERS_MONO,ast.literal_eval(dataFormatted))
client2.close()
#--------------INVERTER 3--------------#
client3 = ModbusClient(host = IP_MONOFACIALES, port = PORT, unit_id = DEV_ID[2], auto_open = True)
values = Read_N_Input_Registers(N_REGS_TRI, INIT_REG_TRI, client3, addTimestamp=True, CustomSeconds=DEV_ID[2]*10)
dataFormatted=formattingRegs(values,False)
checkDir(PATH_TO_SAVE_CVSDIRECTORIES)
writeCSV(PATH_TO_SAVE_CVSDIRECTORIES+str(datetime.date.today())+'/'+str(datetime.date.today())+CSV3,HEADERS_TRI,ast.literal_eval(dataFormatted))
client3.close()
#--------------INVERTER 4--------------#
client4 = ModbusClient(host = IP_BIFACIALES, port = PORT, unit_id = DEV_ID[3], auto_open = True)
values = Read_N_Input_Registers(N_REGS_TRI, INIT_REG_TRI, client4, addTimestamp=True, CustomSeconds=DEV_ID[3]*10)
dataFormatted=formattingRegs(values,False)
checkDir(PATH_TO_SAVE_CVSDIRECTORIES)
writeCSV(PATH_TO_SAVE_CVSDIRECTORIES+str(datetime.date.today())+'/'+str(datetime.date.today())+CSV4,HEADERS_TRI,ast.literal_eval(dataFormatted))
client4.close()














