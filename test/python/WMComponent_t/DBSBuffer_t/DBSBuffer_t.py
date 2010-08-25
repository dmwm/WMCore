#!/usr/bin/env python
"""
DBSBuffer test TestDBSBuffer module and the harness
"""

__revision__ = "$Id: DBSBuffer_t.py,v 1.9 2009/10/13 20:57:42 meloam Exp $"
__version__ = "$Revision: 1.9 $"
__author__ = "anzar@fnal.gov"

import threading
import time
import unittest
import os

from WMComponent.DBSBuffer.DBSBuffer import DBSBuffer

from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMQuality.TestInit import TestInit

class DBSBufferTest(unittest.TestCase):
    """
    TestCase for DBSBuffer module 
    """

    _maxMessage = 10

    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        DBSBuffer tables.  Also add some dummy locations.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()    
        self.config = self.getConfig()
        self.testInit.setSchema(customModules = ["WMComponent.DBSBuffer.Database",
                                                  'WMCore.ThreadPool',
                                                  'WMCore.MsgService',
                                                  'WMCore.Trigger'],
                                useDefault = True)

        return
          
    def tearDown(self):        
        """
        _tearDown_
        
        Drop all the DBSBuffer tables.
        """
        self.testInit.clearDatabase()


    def getConfig(self):
        """
        _getConfig_

        Gets config object for testing
        """
        config = self.testInit.getConfiguration()
        config.component_("DBSBuffer")
        config.DBSBuffer.logLevel = 'INFO'
        config.DBSBuffer.namespace = 'WMComponent.DBSBuffer.DBSBuffer'
        config.DBSBuffer.maxThreads = 1
        config.DBSBuffer.jobSuccessHandler = \
                                           'WMComponent.DBSBuffer.Handler.JobSuccess'


        return config

    def testMultipleJobFrameworkReports(self):
        """
        Mimics creation of component and handles JobSuccess messages with
        many different job framework reports
        """
        myThread = threading.currentThread()
        
        testDBSBuffer = DBSBuffer(self.config)
        
        testDBSBuffer.prepareToStart()
        
        fjr_path = 'FmwkJobReports'
        count = 0;
        for aFJR in os.listdir(fjr_path):
            if aFJR.endswith('.xml') and aFJR.startswith('FrameworkJobReport'):
                count = count + 1
                testDBSBuffer.handleMessage('JobSuccess', fjr_path+'/'+aFJR)
                
        while threading.activeCount() > 1:
        
            print('Currently: '+str(threading.activeCount())+\
                    ' Threads. Wait until all our threads have finished')
            time.sleep(1)

        result = myThread.dbi.processData("SELECT Path FROM dbsbuffer_dataset")[0].fetchall()

        self.assertEqual(len(result), 11)

        testList = []
        for i in result:
            testList.append(i.values()[0])

        assert "/Calo/Commissioning08-v1-merged/RAW" in testList, "Could not find dataset in the result"

        result = myThread.dbi.processData("SELECT * FROM dbsbuffer_file")[0].fetchall()

        self.assertEqual(len(result), 173)


        return
        

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


        result = myThread.dbi.processData("SELECT Path FROM dbsbuffer_dataset")[0].fetchall()

        self.assertEqual(len(result), 5)

        testList = []
        for i in result:
            testList.append(i.values()[0])

        assert "/Calo/Commissioning08-v1-merged/RAW" in testList, "Could not find dataset in the result"


        result = myThread.dbi.processData("SELECT * FROM dbsbuffer_file")[0].fetchall()

        self.assertEqual(len(result), 5)

        result = myThread.dbi.processData("SELECT * FROM dbsbuffer_algo")[0].fetchall()

        #Is this right?  Calo and Commissioning don't have separate algos?
        self.assertEqual(len(result), 4)

        return

if __name__ == '__main__':
    unittest.main()

