#!/usr/bin/env python
"""
DBSBuffer test TestDBSBuffer module and the harness
"""

__revision__ = "$Id: DBSBuffer_t_anzar.py,v 1.13 2009/07/17 15:59:40 sfoulkes Exp $"
__version__ = "$Revision: 1.13 $"
__author__ = "anzar@fnal.gov"

import commands
import logging
import os
import os.path
import threading
import time
import unittest

from WMComponent.DBSBuffer.DBSBuffer import DBSBuffer

from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit

class DBSBufferTest(unittest.TestCase):
    """
    TestCase for DBSBuffer module 
    """

    _setup_done = False
    _teardown = False
    _maxMessage = 10

    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        DBSBuffer tables.  Also add some dummy locations.
        """
        self.config = loadConfigurationFile(os.getenv("WMAGENT_CONFIG"))
        myThread = threading.currentThread()
        myThread.dialect = self.config.CoreDatabase.dialect
        
        self.testInit = TestInit(__file__, myThread.dialect)
        self.testInit.setLogging()

        myThread.dbFactory = DBFactory(myThread.logger, self.config.CoreDatabase.connectUrl)
        myThread.dbi = myThread.dbFactory.connect()
        myThread.transaction = Transaction(myThread.dbi)
        
        self.testInit.setSchema(customModules = ["WMComponent.DBSBuffer.Database"],
                                useDefault = True)

        myThread.transaction = None
        myThread.dbi = None
        myThread.dbFactory = None
        return
          
    def tearDown(self):        
        """
        _tearDown_
        
        Drop all the DBSBuffer tables.
        """
        return
        
        myThread = threading.currentThread()
        
        if myThread.transaction == None:
            myThread.transaction = Transaction(self.dbi)
        
        myThread.transaction.begin()

        factory = WMFactory("DBSBuffer", "WMComponent.DBSBuffer.Database")        
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        destroyworked = destroy.execute(conn = myThread.transaction.conn)

        if not destroyworked:
            raise Exception("Could not complete DBSBuffer tear down.")
        
        myThread.transaction.commit()    
        return

    def testA(self):
        """
        Mimics creation of component and handles JobSuccess messages.
        """
        print "1"

        testDBSBuffer = DBSBuffer(self.config)

        print "2a"
        
        testDBSBuffer.prepareToStart()

        print "3"
 
        fjr_path = 'FmwkJobReports'
        count = 0;
	for aFJR in os.listdir(fjr_path):
            print "weee..."
            if aFJR.endswith('.xml') and aFJR.startswith('FrameworkJobReport'):
                count = count + 1
                testDBSBuffer.handleMessage('JobSuccess', fjr_path+'/'+aFJR)
                
        while threading.activeCount() > 2:
            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')
            time.sleep(1)
        DBSBufferTest._teardown = True

    def testSingleJobFrameworkReport(self):
        """
        This should test the output on one JobFrameworkReport

        
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
            
	#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        myThread = threading.currentThread()
        myThread.logger = logging.getLogger('DBSBufferTest')

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
            myThread.database              = os.getenv("DATABASE")
        else:
            print "ERROR: Could not find database setting in environment!"
            print "ABORT: Cannot start without a database"
            raise 'Exception'
            

        options = {}
        if not os.getenv("DBSOCK") == None:
            options['unix_socket'] = os.getenv("DBSOCK")
        if not os.getenv("DIALECT") == None:
            myThread.dialect = os.getenv("DIALECT")
        else:
            print "No dialect found in environment!  Grabbing from database!"
            if os.getenv("DATABASE").lower().find('oracle') != -1:
                myThread.dialect = 'Oracle'
            elif os.getenv("DATABASE").lower().find('mysql') != -1:
                myThread.dialect = 'MySQL'
            elif os.getenv("DATABASE").lower().find('sqlite') != -1:
                myThread.dialect = 'SQLite'
            else:
                print "Could not parse DATABASE.  Using Oracle"
                myThread.dialect = 'Oracle'

        testDBSBuffer = DBSBuffer(config)
        testDBSBuffer.prepareToStart()


        
        dbFactory = DBFactory(myThread.logger, myThread.database, options)
        myThread.dbi = dbFactory.connect()
        myThread.transaction = Transaction(myThread.dbi)

        #Find the test job
        FJR = os.getcwd() + '/testFrameworkJobReport.xml'
        if not os.path.exists(FJR):
            print "ERROR: Test Framework Job Report %s missing!" %(FJR)
            print "ABORT: Cannot test without test Job Report!"
            raise 'exception'
        testDBSBuffer.handleMessage('JobSuccess', FJR)


    def runTest(self):
        self.testA()
if __name__ == '__main__':
    unittest.main()

