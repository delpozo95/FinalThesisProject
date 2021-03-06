#!/bin/bash

echo "Introduzca los siguientes parámetros de configuración de red:" 
read -p 'IP local: ' ip_local
read -p 'Puerta de enlace: ' gateway
read -p 'Máscara: ' netmask

echo "# interface file auto-generated by buildroot" > /etc/network/interfaces
echo "" >> /etc/network/interfaces
echo "auto lo" >> /etc/network/interfaces
echo "iface lo inet loopback" >> /etc/network/interfaces
echo "" >> /etc/network/interfaces
echo "auto eth0 " >> /etc/network/interfaces
echo "iface eth0 inet static" >> /etc/network/interfaces
echo "  address $ip_local" >> /etc/network/interfaces
echo "  gateway $gateway" >> /etc/network/interfaces
echo "  netmask $netmask" >> /etc/network/interfaces
echo "  dns-search google.com" >> /etc/network/interfaces
echo "  dns-nameservers 8.8.8.8" >> /etc/network/interfaces

echo "Modificado archivo /etc/network/interfaces en configuración STATIC"

mkdir -p /root/crontabss
echo " " > /root/crontabss/crontab.txt
echo "*/10 * * * * /usr/bin/python3 /root/pythonScripts/FinalModbusCode.py" >> /root/crontabss/crontab.txt
echo "51 * * * * /usr/bin/python3 /root/pythonScripts/ftpMQTT.py" >> /root/crontabss/crontab.txt
echo "30 21 * * * /usr/bin/python3 /root/pythonScripts/deleteOldCSVConceCom.py" >> /root/crontabss/crontab.txt

chmod 755 /root/pythonScripts/FinalModbusCode.py

echo "#!/bin/bash" > /etc/init.d/S92Carlos
echo " " >> /etc/init.d/S92Carlos
echo "echo  \"nameserver 8.8.8.8\" > /etc/resolv.conf" >> /etc/init.d/S92Carlos
echo "echo \" \" >> /etc/resolv.conf" >> /etc/init.d/S92Carlos
echo "mkdir -p /var/spool/cron/crontabs" >> /etc/init.d/S92Carlos
echo "crontab /root/crontabss/crontab.txt" >> /etc/init.d/S92Carlos


echo $(chmod 755 /etc/init.d/S92Carlos)

echo "Configuracion acabada de DNS. Se requiere reboot"
