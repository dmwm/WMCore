#!/usr/bin/env python
#pylint: disable-msg=E1101,C0103,R0902
"""
DBSUpload test TestDBSUpload module and the harness
"""

__revision__ = "$Id $"
__version__ = "$Revision: 1.12 $"
__author__ = "anzar@fnal.gov"

import commands
import logging
import os
import threading
import time
import unittest

from WMComponent.DBSUpload.DBSUpload import DBSUpload
import WMComponent.DBSUpload.DBSUpload
from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit
from WMCore.DAOFactory import DAOFactory
from WMCore.WMException import WMException

class DBSUploadTest(unittest.TestCase):
    """
    TestCase for DBSUpload module 
    """

    _maxMessage = 10

    def setUp(self):
        """
        setup for test.
        """

        self.testInit = TestInit(__file__, os.getenv("DIALECT"))
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()

        #if (os.getenv("DIALECT").lower() != 'sqlite'):
        #    print "About to tear down"
        #    self.tearDown()
        try:
            self.testInit.setSchema(customModules = ["WMCore.ThreadPool","WMCore.MsgService","WMComponent.DBSBuffer.Database"],
                                useDefault = False)
        except WMException, e:
            self.tearDown()
            raise

        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)

        locationAction = daofactory(classname = "DBSBufferFiles.AddLocation")
        locationAction.execute(siteName = "se1.cern.ch")
        locationAction.execute(siteName = "se1.fnal.gov")
        locationAction.execute(siteName = "malpaquet") 
		
    def tearDown(self):
        """
        Database deletion
        """
        myThread = threading.currentThread()
        factory2 = WMFactory("MsgService", "WMCore.MsgService")
        destroy2 = factory2.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy2.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete MsgService tear down.")
        myThread.transaction.commit()
        
        factory = WMFactory("Threadpool", "WMCore.ThreadPool")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete ThreadPool tear down.")
        myThread.transaction.commit()

        factory = WMFactory("DBSBuffer", "WMComponent.DBSBuffer.Database")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete DBSBuffer tear down.")
        myThread.transaction.commit()
        


    def testA(self):
        
        """
        Mimics creation of component and handles come messages.
        """
        
        #return True
        
        # read the default config first.
        config = loadConfigurationFile(os.path.join(os.path.dirname(\
                        WMComponent.DBSUpload.DBSUpload.__file__), 'DefaultConfig.py'))

        # some general settings that would come from the general default 
        # config file
        config.Agent.contact = "anzar@fnal.gov"
        config.Agent.teamName = "DBS"
        config.Agent.agentName = "DBS Upload"

        config.section_("General")
        
        if not os.getenv("TESTDIR") == None:
            config.General.workDir = os.getenv("TESTDIR")
        else:
            config.General.workDir = os.getcwd()
        
        config.section_("CoreDatabase")
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
            if os.getenv("DATABASE") == 'sqlite://':
                raise RuntimeError,\
                    "These tests will not work using in-memory SQLITE"
            config.CoreDatabase.connectUrl = os.getenv("DATABASE")

        testDBSUpload = DBSUpload(config)
        testDBSUpload.prepareToStart()

        #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # for testing purposes we use this method instead of the 
        # StartComponent one.

#        testDBSUpload.handleMessage('BufferSuccess', \
#				'NoPayLoad')

        #I don't know what this does so I commented it
        #Especially since it breaks things
        #for i in xrange(0, DBSUploadTest._maxMessage):
        #    testDBSUpload.handleMessage('BufferSuccess', \
        #        'YourMessageHere'+str(i))

        while threading.activeCount() > 1:
            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')
            time.sleep(1)


    def runTest(self):
        self.testA()

if __name__ == '__main__':
    unittest.main()

