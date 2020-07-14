from pyModbusTCP.client import ModbusClient
import csv
import time

# Funciones que leen n registros
def Read_N_Input_Registers(quantity, initReg = 0, client=None, maxPetReg=125):
    total = []
    limit = initReg + quantity
    currentReg = initReg
    while currentReg < limit:
        toRead = min(maxPetReg, limit - currentReg)
        #parcial = client.read_input_registers(currentReg, toRead)
        parcial=client.read_input_registers(currentReg,1)
        if parcial is None:
            #print("None here")
            #return None
            parcial="nada"
        #total += parcial
        total.append(parcial)
        currentReg += 1
        #time.sleep(0.05)
    return total
    #return parcial

def Read_N_Holding_Registers(quantity, initReg = 0, client=None, maxPetReg=125):
    total = []
    limit = initReg + quantity
    currentReg = initReg
    while currentReg < limit:
        toRead = min(maxPetReg, limit - currentReg)
        parcial = client.read_holding_registers(currentReg,1)
        #parcial = client.read_holding_registers(currentReg, toRead)
        if parcial is None:
            #return None
            parcial="nada"
        #total += parcial
        total.append(parcial)
        currentReg += 1
    return total

'''
    toWrite is a list of list with this format: [[registryAddress,newValue][registryAddress,newValue]]
'''
def Write_N_Registers(toWrite =[],client = None):
    totalChecks = []
    for i in range(0,len(toWrite)):
        parcial=client.write_single_register(toWrite[i][0],toWrite[i][1])
        if parcial is None:
            return (i,None)
        totalChecks.append([i,parcial])
    return totalChecks

def writeCSV(fileName,columnsNames,data,headers=False):
    with open (fileName, mode='+a', newline='') as csv_file:
        try:
            writer = csv.DictWriter(csv_file, fieldnames=columnsNames)
            if (headers==True):
                writer.writeheader()
            for i in data:
                writer.writerow(i)
        except csv.Error as e:
            print("Error"+str(e))
        except Exception as e:
            print("Error"+str(e))

def writeDefault():
    inverter=int(input("Choose inverter number to clean its CSV:"))
    with open (CSV[inverter-1], mode='w', newline='') as csv_file:
        fields=['RegNumber', 'Value']
        writer = csv.DictWriter(csv_file, fieldnames=fields)
        writer.writeheader()
        
# Datos del cliente modbus
#IP1     = 'localhost'
IP1     = 'X.X.X.X.X'
IP2     = 'X.X.X.X.X'
IP3     = 'X.X.X.X.X'
IP4     = 'X.X.X.X.X' 
IP = [IP1,IP2,IP3,IP4]
CSV1 = 'Inverter1.csv'
CSV2 = 'Inverter2.csv'
CSV3 = 'Inverter3.csv'
CSV4 = 'Inverter4.csv'
CSV = [CSV1,CSV2,CSV3,CSV4]
PORT   = 502
DEV_ID1 = 1
DEV_ID2 = 2
DEV_ID3 = 3
DEV_ID4 = 4
DEV_ID = [DEV_ID1,DEV_ID2,DEV_ID3,DEV_ID4]

# Registros
INIT_REGISTER = 0
N_REGISTERS   = 200
#TO_CHANGE = [[1,14],[5,44]]

# Conexion

inverter=int(input("Inverter number: "))
if(inverter==1234):
    writeDefault()
else:
    client = ModbusClient(host = IP[inverter-1], port = PORT, unit_id = DEV_ID[inverter-1], auto_open = True)
    #value = Read_N_Input_Registers(N_REGISTERS, INIT_REGISTER, client)
    value = Read_N_Holding_Registers(N_REGISTERS, INIT_REGISTER, client)
    #print(value)
    registers=[]
    for i in range(INIT_REGISTER,N_REGISTERS):
        data=[{"RegNumber":str(i),"Value":str(value[i])}]
        #print(data)
        writeCSV(CSV[inverter-1],['RegNumber','Value'],data,headers=False)
        registers.append(data)
print("END")




# Elegir en funcion del tipo de funcion requerida
#result = Read_N_Input_Registers(N_REGISTERS, INIT_REGISTER, client)
'''result = Read_N_Holding_Registers(N_REGISTERS, INIT_REGISTER, client)
print(result)
result2 = Write_N_Registers(TO_CHANGE, client)
print(result2)
result = Read_N_Holding_Registers(N_REGISTERS, INIT_REGISTER, client)
print(result)'''














