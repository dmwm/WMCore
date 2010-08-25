#!/usr/bin/env python
"""
_DBSBufferDataset_t_

Unit tests for manipulating datasets in DBSBuffer.
"""

__revision__ = "$Id: DBSBufferDataset_t.py,v 1.1 2009/06/12 19:17:09 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

import unittest
import logging
import os
import commands
import threading
import random
from sets import Set

from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.DAOFactory import DAOFactory
from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit

class DBSBufferDatasetTest(unittest.TestCase):
    _setup = False
    _teardown = False

    def runTest(self):
        """
        _runTest_

        Run all the unit tests.
        """
        unittest.main()
    
    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        DBSBuffer tables.
        """
        if self._setup:
            return

        self.testInit = TestInit(__file__, os.getenv("DIALECT"))
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMComponent.DBSBuffer.Database"],
                                useDefault = False)

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)

        self._setup = True
        return
          
    def tearDown(self):        
        """
        _tearDown_
        
        Drop all the DBSBuffer tables.
        """
        myThread = threading.currentThread()
        
        if self._teardown:
            return

        if myThread.transaction == None:
            myThread.transaction = Transaction(self.dbi)
        
        myThread.transaction.begin()

        factory = WMFactory("DBSBuffer", "WMComponent.DBSBuffer.Database")        
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        destroyworked = destroy.execute(conn = myThread.transaction.conn)

        if not destroyworked:
            raise Exception("Could not complete DBSBuffer tear down.")
        
        myThread.transaction.commit()    
        self._teardown = True
            
    def testCreate(self):
        """
        _testCreate_

        Verify that the creation of a dataset in the DBSBuffer works
        correctly.
        """
        newDatasetAction = self.daoFactory(classname = "NewDataset")
        listDatasetAction = self.daoFactory(classname = "ListDataset")
        
        newDatasetAction.execute(datasetPath = "/Cosmics/CRUZET-v1/RECO")
        resultA = listDatasetAction.execute(datasetPath = "/Cosmics/CRUZET-v1/RECO")

        assert len(resultA) == 1, \
               "ERROR: Wrong number of datasets returned: %s" % len(resultA)
        assert resultA[0]["path"] == "/Cosmics/CRUZET-v1/RECO", \
               "ERROR: Wrong dataset path in DBSBuffer"

        newDatasetAction.execute(datasetPath = "/Cosmics/CRUZET-v1/RECO")
        resultB = listDatasetAction.execute(datasetID = resultA[0]["id"])

        assert len(resultB) == 1, \
               "ERROR: Wrong number of datasets returned: %s" % len(resultB)
        assert resultB[0]["path"] == "/Cosmics/CRUZET-v1/RECO", \
               "ERROR: Wrong dataset path in DBSBuffer"

        return

    def testCreateTransaction(self):
        """
        _testCreateTransaction_

        Verify that the new dataset DAO object handles transactions correctly.
        """
        myThread = threading.currentThread()
        myThread.transaction.begin()

        newDatasetAction = self.daoFactory(classname = "NewDataset")
        listDatasetAction = self.daoFactory(classname = "ListDataset")
        
        newDatasetAction.execute(datasetPath = "/Cosmics/CRUZET-v1/RECO",
                                 conn = myThread.transaction.conn,
                                 transaction = True)
        resultA = listDatasetAction.execute(datasetPath = "/Cosmics/CRUZET-v1/RECO",
                                            conn = myThread.transaction.conn,
                                            transaction = True)

        assert len(resultA) == 1, \
               "ERROR: Wrong number of datasets returned: %s" % len(resultA)
        assert resultA[0]["path"] == "/Cosmics/CRUZET-v1/RECO", \
               "ERROR: Wrong dataset path in DBSBuffer"

        myThread.transaction.rollback()

        resultB = listDatasetAction.execute(datasetID = resultA[0]["id"],
                                            conn = myThread.transaction.conn,
                                            transaction = True)
                                
        assert len(resultB) == 0, \
               "ERROR: Wrong number of datasets returned: %s" % len(resultB)

        return

if __name__ == "__main__":
    unittest.main() 
