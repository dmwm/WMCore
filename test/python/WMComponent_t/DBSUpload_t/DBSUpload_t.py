#!/usr/bin/env python
#pylint: disable-msg=E1101,C0103,R0902
"""
DBSUpload test TestDBSUpload module and the harness
"""

__revision__ = "$Id $"
__version__ = "$Revision: 1.14 $"
__author__ = "anzar@fnal.gov"


import os
import threading
import time
import unittest

from WMComponent.DBSUpload.DBSUpload import DBSUpload
import WMComponent.DBSUpload.DBSUpload
from WMQuality.TestInit import TestInit
from WMCore.DAOFactory import DAOFactory


class DBSUploadTest(unittest.TestCase):
    """
    TestCase for DBSUpload module 
    """

    _maxMessage = 10

    def setUp(self):
        """
        setup for test.
        """

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = 
                                ["WMCore.ThreadPool",
                                 "WMCore.MsgService",
                                 "WMComponent.DBSBuffer.Database"],
                                useDefault = False)

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
        self.testInit.clearDatabase()


    def testA(self):
        
        """
        Mimics creation of component and handles come messages.
        """
        
        #return True
        
        # read the default config first.
        config = self.testInit.getConfiguration(os.path.join(os.path.dirname(\
                        WMComponent.DBSUpload.DBSUpload.__file__), 'DefaultConfig.py'))

 
        testDBSUpload = DBSUpload(config)
        testDBSUpload.prepareToStart()

        while threading.activeCount() > 1:
            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')
            time.sleep(1)

if __name__ == '__main__':
    unittest.main()

