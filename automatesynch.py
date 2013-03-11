#!/usr/bin/python
import os
import subprocess
import subprocess as sub
import MySQLdb
import tempfile
import logging
import shutil
import datetime
import tarfile

#OPERATIONS
MYSQL_DUMP = "mysqldump"

# MASTER DATA
masterHost   = '127.0.0.1'
masterUser   = 'root'
masterPasswd = 'india611'
masterPort   = 3306

# SLAVE DATA
slaveHost    = '127.0.0.1'
slavePort    = 3307
slaveUser    = 'root'
slavePasswd  = 'india611'

logging.basicConfig(
                    level=    logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='/home/mani/vre_synch1.txt'
                   )
#dbMasterCon = MySQLdb.connect(host='127.0.0.1', user='root', passwd='india611', port=3306)
dbMasterCon = MySQLdb.connect(host= masterHost, user = masterUser, passwd = masterPasswd, port = masterPort)
logging.info("DB Connected")
cur = dbMasterCon.cursor()
cur.execute("RESET MASTER")
cur.execute("FLUSH TABLES WITH READ LOCK")
showMaster = 'mysql -h 127.0.0.1 -P 3306 -u root -p --skip-column-names -e \'SHOW MASTER STATUS;\''
p = os.popen(showMaster,'r', 1)
str1 = p.read()
str2 = str1.split()
masterFile = str2[0]
masterPOS  = str2[1]
f = tempfile.mktemp()
tempfile = open(f, 'w')
tempfile.write('STOP SLAVE; \n')
#takeaDUMP = 'mysqldump -h 127.0.0.1 -P 3306 --all-database --add-drop-table --add-drop-database -u root -p'
takeaDUMP = 'mysqldump -h '+ str(masterHost)+ ' -P '+ str(masterPort) + ' --all-database --add-drop-table --add-drop-database ' + '-u ' + str(masterUser) + ' -p'
#print takeaDUMP
p1 = os.popen(takeaDUMP,'r',1)
strDUMP = p1.read()
tempfile.write(strDUMP + '\n')
tempfile.write('CHANGE MASTER TO MASTER_HOST = ' + str(masterHost) + '\n')
tempfile.write('MASTER_PORT = '+ str(masterPort) + '\n')
tempfile.write('MASTER_USER = '+ str(masterUser) + '\n')
tempfile.write('MASTER_PASSWORD = '+ str(masterPasswd) + '\n')
tempfile.write('MASTER_LOG_FILE = '+ masterFile + '\n' )
tempfile.write('MASTER_LOG_POS = '+ masterPOS + '\n' )
tempfile.write('START SLAVE; \n')
tempfile.close()
#tar.close()
cur.execute("UNLOCK TABLES")
#print f
# SCP File to Slave 
#os.system("scp f mani@localhost:/home/mani")
filepath = f
hostname = "mani@localhost"
remote_path = "/home/mani"
subprocess.call(['scp', filepath, ':'.join([hostname,remote_path])])

# SLAVE System
#subprocess.call(['ssh', '-i', remote_user +'@'+remote_host,'mysql -u root -p '])
#dbSlaveCon = MySQLdb.connect(host=slaveHost, user=slaveUser, passwd=slavePasswd, port=slavePort)
#cur = dbSlaveCon.cursor()
    #for line in open('')
     #   cur.execute(line)
#logging.info("Replication complete")
remote_user = 'mani'
remote_host = 'localhost'
remote_pass = 'india611'
slavePort = '3307'
#print ' < /home/mani/' + os.path.basename(f)  

#subprocess.call([
                #'ssh'+
                # remote_user + '@' + remote_host,
                # '/usr/bin/mysql -h ' + remote_host + '-P ' + slavePort + '-u root -p'+
                # ' < /home/mani/' + os.path.basename(f)  
               #])
#tt = 'ssh '+ remote_user + '@' + remote_host +'mysql -h ' + remote_host + ' -P ' + slavePort + ' -u root -p'+remote_pass+ '< /home/mani/' + os.path.basename(f)             
#print tt;
#print os.path.basename(f)
#tt = 'ssh '+ remote_user + '@' + remote_host,' mysql -h ' + remote_host + ' -P ' + slavePort + ' -u root -p'+remote_pass+ ' < /home/mani/' + os.path.basename(f)  
#print tt  

tt = '\'mysql -h ' + '127.0.0.1' + ' -P ' + slavePort + ' -u root -p'+remote_pass+ ' < /home/mani/' + os.path.basename(f) +'\''  
print tt

subprocess.call([
                'ssh '+
                 remote_user + '@' + remote_host, 
                 tt  
               ])
