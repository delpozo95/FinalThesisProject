from ftplib import FTP_TLS
import datetime
from io import StringIO

#connection
ftps = FTP_TLS('HOST')
ftps.login('user', 'password')           # login anonymously before securing control channel
ftps.prot_p()          # switch to secure data connection

ftps.retrlines('LIST')
again=1
while(again==1):
    value=int(input("  1-LIST\n  2-CD\n  3-READ\n  4-EXIT: "))
    if(value==1):
        ftps.retrlines('LIST')
    elif(value==2):
        route=str(input("Where?: "))
        try:
            ftps.cwd(route)
            print('Now you are inside'+route)
        except ValueError:
            print('Error: ')
    elif(value==3):
        filee=str(input("What file?: "))
        rrr = StringIO()
        try:
            ftps.retrlines('RETR '+filee,rrr.write)
            print(rrr.getvalue())
        except ValueError:
            print('Error: ')
    elif(value==4):
        again=0
    print('\n')
ftps.quit()
