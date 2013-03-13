#!/usr/bin/python

# IMPORTS
import os
import subprocess
import MySQLdb
import tempfile
import logging

# METADATA
__author__ = "Chaudhry Usman Ali"
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Chaudhry Usman Ali"
__email__ = "mani.ali@unb.ca"
__status__ = "Development"

# INFORMATION
# MASTER DATA
print "********************"
print "Enter MASTER DETAILS"
print "********************"
masterHost = raw_input('Enter the Master MySQL Host: ')
masterUser = raw_input('Enter the Master MySQL User: ')
masterPasswd = raw_input('Enter the Master MySQL Password: ')
masterPort = raw_input('Enter the Master MySQL Port: ')
print "********************"
print "Enter REMOTE DETAILS"
print "********************"
# REMOTE USER INFORMATION
remote_user = raw_input('Enter the Remote User Name: ')
remote_host = raw_input('Enter the Remote User Host: ')
remote_user_auth_creds={}
remote_user_auth_creds['key']='/home/mani/.ssh/rocknroll'
print "********************"
print "Enter SLAVE DETAILS"
print "********************"
# SLAVE DATA
slaveHost = raw_input('Enter the Slave MySQL Host: ')
slavePort = raw_input('Enter the Slave MySQL Port: ')
slaveUser = raw_input('Enter the Slave MySQL User: ')
slavePasswd = raw_input('Enter the Slave MySQL Password: ')

# PRINT INFORMATION
print "========= MASTER INFO ==========="
print "MASTER INFO"
print "Master Host: "+ masterHost
print "Master User: "+ masterUser
print "Master Password: "+ masterPasswd
print "Master Port: "+ masterPort

print "===========SLAVE INFO============="
print "Remote User: "+remote_user
print "Remote Host: "+remote_host

print "===========SLAVE INFO============="
print "SLAVE INFO"
print "Slave Host: "+ slaveHost
print "Slave User: "+ slaveUser
print "Slave Password: "+ slavePasswd
print "Slave Port: "+ slavePort


# LOGGING
logging.basicConfig(
                    level= logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='/home/mani/vre_synch1.txt'
                   )

# Establish Master DB Connectivity
dbMasterCon = MySQLdb.connect(host= masterHost, user = masterUser, passwd = masterPasswd, port = int(masterPort))
print dbMasterCon
logging.info("DB Connected")
cur = dbMasterCon.cursor()
cur.execute("RESET MASTER")
cur.execute("FLUSH TABLES WITH READ LOCK")

# SHOW MASTER STATUS - Grab masterFile and masterPOS for use in file to be sent to the Slave
# SHOW Value without columns
showMaster = 'mysql -h '+ masterHost+ ' -P '+masterPort+' -u '+masterUser+ ' -p'+masterPasswd+' --skip-column-names -e'+'\'SHOW MASTER STATUS;\'' 

# Grab values for File and Position
p = os.popen(showMaster,'r', 1)
str1 = p.read()
str2 = str1.split()
masterFile = str2[0]
masterPOS  = str2[1]

f = tempfile.mktemp()
tempfile = open(f, 'w')
tempfile.write('STOP SLAVE; \n')

# SQL Dump
takeaDUMP = 'mysqldump -h '+ str(masterHost)+ ' -P '+ str(masterPort) + ' --all-database --add-drop-table --add-drop-database ' + '-u ' + str(masterUser) + ' -p'+ masterPasswd

# Push DUMP into the file
p1 = os.popen(takeaDUMP,'r',1)
strDUMP = p1.read()
tempfile.write(strDUMP + '\n')

# Push Instructions for the slave
tempfile.write('CHANGE MASTER TO MASTER_HOST = ' +'\''+ str(masterHost)+ '\','+ '\n')
tempfile.write('MASTER_PORT = '+ str(masterPort) +','+'\n')
tempfile.write('MASTER_USER = '+ '\''+str(masterUser) +'\''+','+'\n')
tempfile.write('MASTER_PASSWORD = '+'\''+ str(masterPasswd) +'\''+','+'\n')
tempfile.write('MASTER_LOG_FILE = '+ '\''+masterFile +'\''+','+'\n' )
tempfile.write('MASTER_LOG_POS = '+ masterPOS +';'+ '\n' )
tempfile.write('START SLAVE; \n')
# Close file
tempfile.close()

cur.execute("UNLOCK TABLES")

# SCP File Configuration
filepath = f
hostname = '127.0.0.1'
port = 22
username = 'mani'
password = 'Change Value Here'
remote_path = "/home/mani/Downloads/"
subprocess.call(['scp', filepath, ':'.join([hostname,remote_path])])

# REMOTE HOST OPERATIONS
command = 'mysql -h '+slaveHost+' -P '+slavePort+' -u '+slaveUser+' -p'+slavePasswd+ ' < '+remote_path+os.path.basename(f) 

pushintoRemote = subprocess.call([
                 'ssh',
                 '-i',remote_user_auth_creds['key'],
                 remote_user+'@'+remote_host,
                 command
               ])
# CHECK Exit Status
print pushintoRemote