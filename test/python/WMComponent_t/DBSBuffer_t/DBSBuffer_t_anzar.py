#!/usr/bin/env python
#pylint: disable-msg=E1101,C0103,R0902
"""
DBSBuffer test TestDBSBuffer module and the harness
"""

__revision__ = "$Id: DBSBuffer_t_anzar.py,v 1.11 2009/05/15 15:46:12 mnorman Exp $"
__version__ = "$Revision: 1.11 $"
__author__ = "anzar@fnal.gov"

import commands
import logging
import os
import threading
import time
import unittest

from WMComponent.DBSBuffer.DBSBuffer import DBSBuffer

from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMFactory import WMFactory

class DBSBufferTest(unittest.TestCase):
    """
    TestCase for DBSBuffer module 
    """

    _setup_done = False
    _teardown = False
    _maxMessage = 10

    def setUp(self):
        """
        setup for test.
        """
        
	if not DBSBufferTest._setup_done:
		logging.basicConfig(level=logging.NOTSET,
                	format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                	datefmt='%m-%d %H:%M',
                	filename='%s.log' % __file__,
                	filemode='w')

            	myThread = threading.currentThread()
            	myThread.logger = logging.getLogger('DBSBufferTest')
            	#myThread.dialect = 'MySQL'
                myThread.dialect = os.getenv("DIALECT")

            	options = {}
                if not os.getenv("DBSOCK") == None:
                    options['unix_socket'] = os.getenv("DBSOCK")
            	dbFactory = DBFactory(myThread.logger, os.getenv("DATABASE"), \
                	options)

                print os.getenv("DATABASE")

            	myThread.dbi = dbFactory.connect()
            	myThread.transaction = Transaction(myThread.dbi)
                myThread.transaction.begin()
                #myThread.transaction.commit()
                createworked=0
		
            	# need to create these tables for testing.
            	factory = WMFactory("msgService", "WMCore.MsgService."+ \
                	myThread.dialect)
            	create = factory.loadObject("Create")
		try: 
            		createworked = create.execute(conn = myThread.transaction.conn)
		except Exception, ex:
                        if ex.__str__().find("already exists") != -1 :
				print "WARNING: Table Already Exists Exception Raised, All Tables may not have been created"
                                pass
                        else:
                                raise ex
            	if createworked:
                	logging.debug("MsgService tables created")
            	else:
                	logging.debug("MsgService tables could not be created, \
                    	already exists?")

                
            	# as the example uses threads we need to create the thread
            	# tables too.

            	factory = WMFactory("msgService", "WMCore.ThreadPool."+ \
                	myThread.dialect)
            	create = factory.loadObject("Create")
		try:
            		createworked = create.execute(conn = myThread.transaction.conn)
                except Exception, ex:
                        if ex.__str__().find("already exists") != -1 :
                                print "WARNING: Table Already Exists Exception Raised, All Tables may not have been created"
                                pass
                        else:
                                raise ex
            	if createworked:
                	logging.debug("ThreadPool tables created")
            	else:
                	logging.debug("ThreadPool tables could not be created, \
                    	already exists?")


            	# need to create DBSBuffer tables for testing.
            	factory = WMFactory("dbsBuffer", "WMComponent.DBSBuffer.Database."+ \
                	myThread.dialect)
            	create = factory.loadObject("Create")
		try:
                	createworked = create.execute(conn = myThread.transaction.conn)
                except Exception, ex:
                        if ex.__str__().find("already exists") != -1 :
                                print "WARNING: Table Already Exists Exception Raised, All Tables may not have been created"
                                pass
                        else:
                                raise ex
            	if createworked:
                	logging.debug("DBSBuffer tables created")
            	else:
                	logging.debug("DBSBuffer tables could not be created, \
                    	already exists?")
                
                # Throw a message
                factory = WMFactory("msgService", "WMCore.MsgService."+ \
                        myThread.dialect)
                newMsgService = factory.loadObject("MsgService")
                newMsgService.registerAs("DBSBufferTestComp")

               	msg = {'name':'JobSuccess', 'payload':'/uscms/home/anzar/work/FJR/forAnzar/Run68141/Calo/FrameworkJobReport-30.xml'}
               	newMsgService.publish(msg)

                newMsgService.finish()
                
                myThread.transaction.commit()
                
                
                DBSBufferTest._setup_done = True

    def tearDown(self):
        """
        Database deletion
        """
	
	# For testing not deleteing anything from Database yet
	return True

        myThread = threading.currentThread()
        if DBSBufferTest._teardown and myThread.dialect == 'MySQL':
            command = 'mysql -u root '
	    +' --socket='\
            + os.getenv('TESTDIR') \
            + '/mysqldata/mysql.sock --exec "drop database ' \
            + os.getenv('DBNAME')+ '"'
            commands.getstatusoutput(command)

            command = 'mysql -u root --socket=' \
            + os.getenv('TESTDIR')+'/mysqldata/mysql.sock --exec "' \
            + os.getenv('SQLCREATE') + '"'
            commands.getstatusoutput(command)

            command = 'mysql -u root --socket=' \
            + os.getenv('TESTDIR') \
            + '/mysqldata/mysql.sock --exec "create database ' \
            +os.getenv('DBNAME')+ '"'
            commands.getstatusoutput(command)
        DBSBufferTest._teardown = False


    def testA(self):
        """
        Mimics creation of component and handles JobSuccess messages.
        """

        # read the default config first.
        config = loadConfigurationFile(os.path.join(os.getenv('WMCOREBASE'), \
            'src/python/WMComponent/DBSBuffer/DefaultConfig.py'))

        # some general settings that would come from the general default 
        # config file
        config.Agent.contact = "anzar@fnal.gov"
        config.Agent.teamName = "DBS"
        config.Agent.agentName = "DBS Buffer"

        config.section_("General")
        if not os.getenv("TESTDIR") == None:
            config.General.workDir = os.getenv("TESTDIR")
        else:
            config.General.workDir = os.getcwd()

        config.section_("CoreDatabase")
        config.CoreDatabase.dialect = 'mysql'
        if not os.getenv("DIALECT") == None:
            config.CoreDatabase.dialect = os.getenv("DIALECT").lower()
        #config.CoreDatabase.socket = os.getenv("DBSOCK")
        if not os.getenv("DBUSER") == None:
            config.CoreDatabase.user = os.getenv("DBUSER")
        else:
            config.CoreDatabase.user = os.getenv("USER")
        if not os.getenv("DBHOST") == None:
            config.CoreDatabase.hostname = os.getenv("DBHOST")
        else:
            config.CoreDatabase.hostname = os.getenv("HOSTNAME")
        config.CoreDatabase.passwd = os.getenv("DBPASS")
        if not os.getenv("DBNAME") == None:
            config.CoreDatabase.name = os.getenv("DBNAME")
        else:
            config.CoreDatabase.name = os.getenv("DATABASE")
        if not os.getenv("DATABASE") == None:
            config.CoreDatabase.connectUrl = os.getenv("DATABASE")

	#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        myThread = threading.currentThread()
        myThread.logger = logging.getLogger('DBSBufferTest')
        myThread.dialect = os.getenv("DIALECT")

        options = {}
        if not os.getenv("DBSOCK") == None:
            options['unix_socket'] = os.getenv("DBSOCK")
        dbFactory = DBFactory(myThread.logger, os.getenv("DATABASE"), \
                options)

        testDBSBuffer = DBSBuffer(config)
        testDBSBuffer.prepareToStart()
 
        myThread.dbi = dbFactory.connect()
        myThread.transaction = Transaction(myThread.dbi)
        #########myThread.transaction.begin()
        #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # for testing purposes we use this method instead of the
        # StartComponent one.
        #testDBSBuffer.handleMessage('JobSuccess', \
        #        'C:\WORK\FJR\fjr_01.xml')

	print "HAVE YOU DONE,  insert into dbsbuffer_location (id, se_name) values (1, 'srm.cern.ch'); ?????"

	"""
        for fjr_dir in ['Calo', 'MinimumBias', 'Cosmics']: 	
	    fjr_path='/uscms/home/anzar/work/FJR/forAnzar/Run67838/'+fjr_dir
	    for aFJR in os.listdir(fjr_path):
		if aFJR.endswith('.xml'):
			testDBSBuffer.handleMessage('JobSuccess', fjr_path+'/'+aFJR)
	"""

	fjr_path='/uscms/home/anzar/work/FJR/forAnzar/Run67838'
        count = 0;
	for aFJR in os.listdir(fjr_path):
            if myThread.dialect.lower() == 'oracle' and count > 10:
                continue
            if aFJR.endswith('.xml') and aFJR.startswith('FrameworkJobReport'):
                count = count + 1
                testDBSBuffer.handleMessage('JobSuccess', fjr_path+'/'+aFJR)
                


			
        #########myThread.transaction.commit()
         
        while threading.activeCount() > 1:
            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')
            time.sleep(1)
        DBSBufferTest._teardown = True

    def runTest(self):
        self.testA()
if __name__ == '__main__':
    unittest.main()

