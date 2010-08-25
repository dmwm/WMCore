#!/usr/bin/env python
"""
DBSBuffer test TestDBSBuffer module and the harness
"""

__revision__ = "$Id: DBSBuffer_t_anzar.py,v 1.15 2009/08/13 19:41:16 meloam Exp $"
__version__ = "$Revision: 1.15 $"
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

from WMCore.Agent.Configuration import Configuration

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
        #self.config = loadConfigurationFile(os.getenv("WMAGENT_CONFIG"))
        self.config = self.getConfig()
        myThread = threading.currentThread()
        myThread.dialect = self.config.CoreDatabase.dialect
        
        self.testInit = TestInit(__file__, myThread.dialect)
        self.testInit.setLogging()

        myThread.dbFactory = DBFactory(myThread.logger, self.config.CoreDatabase.connectUrl)
        myThread.dbi = myThread.dbFactory.connect()
        myThread.transaction = Transaction(myThread.dbi)

        #self.tearDown()
        
        self.testInit.setSchema(customModules = ["WMComponent.DBSBuffer.Database", 'WMCore.ThreadPool','WMCore.MsgService','WMCore.Trigger'],
                                useDefault = True)

        #myThread.transaction = None
        #myThread.dbi = None
        #myThread.dbFactory = None
        return
          
    def tearDown(self):        
        """
        _tearDown_
        
        Drop all the DBSBuffer tables.
        """
        
        myThread = threading.currentThread()
        
        if myThread.transaction == None:
            myThread.transaction = Transaction(myThread.dbi)
        
        myThread.transaction.begin()

        factory = WMFactory("DBSBuffer", "WMComponent.DBSBuffer.Database")        
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete DBSBuffer tear down.")

        factory = WMFactory("DBSBuffer", "WMCore.ThreadPool")        
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete ThreadPool tear down.")

        factory = WMFactory("DBSBuffer", "WMCore.MsgService")        
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete MsgService tear down.")

        factory = WMFactory("DBSBuffer", "WMCore.Trigger")        
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete Trigger tear down.")
        
        myThread.transaction.commit()    
        return


    def getConfig(self):
        """
        _getConfig_

        Gets config object for testing
        """

        myThread = threading.currentThread()

        config = Configuration()
        config.component_("DBSBuffer")
        config.DBSBuffer.logLevel = 'INFO'
        config.DBSBuffer.namespace = 'WMComponent.DBSBuffer.DBSBuffer'
        config.DBSBuffer.maxThreads = 1
        config.DBSBuffer.jobSuccessHandler = \
                                           'WMComponent.DBSBuffer.Handler.JobSuccess'

        config.section_("General")
        
        if not os.getenv("TESTDIR") == None:
            config.General.workDir = os.getenv("TESTDIR")
        else:
            config.General.workDir = os.getcwd()
        
        config.section_("CoreDatabase")
        if not os.getenv("DIALECT") == None:
            config.CoreDatabase.dialect = os.getenv("DIALECT")
            myThread.dialect = os.getenv('DIALECT')
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
            myThread.database = os.getenv("DATABASE")


        return config

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
                
        while threading.activeCount() > 1:
        
            print('Currently: '+str(threading.activeCount())+\
                    ' Threads. Wait until all our threads have finished')
            time.sleep(1)

    def testSingleJobFrameworkReport(self):
        """
        This should test the output on one JobFrameworkReport

        
        """

        
        myThread = threading.currentThread()

        testDBSBuffer = DBSBuffer(self.config)
        testDBSBuffer.prepareToStart()

        options = {} 	 
        if not os.getenv("DBSOCK") == None: 	 
            options['unix_socket'] = os.getenv("DBSOCK") 	 
        
        dbFactory = DBFactory(myThread.logger, myThread.database, options)
        myThread.dbi = dbFactory.connect()
        myThread.transaction = Transaction(myThread.dbi)

        #Find the test job
        FJR = os.getcwd() + '/FmwkJobReports/FrameworkJobReport-4562.xml'
        if not os.path.exists(FJR):
            print "ERROR: Test Framework Job Report %s missing!" %(FJR)
            print "ABORT: Cannot test without test Job Report!"
            raise Exception
        testDBSBuffer.handleMessage('JobSuccess', FJR)

        while threading.activeCount() > 1:
            
            print('Currently: '+str(threading.activeCount())+\
                  ' Threads. Wait until all our threads have finished')
            time.sleep(1)


        return

if __name__ == '__main__':
    unittest.main()

