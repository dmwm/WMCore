#!/usr/bin/env python
"""
_DBSBufferDataset_t_

Unit tests for manipulating datasets in DBSBuffer.
"""

__revision__ = "$Id: DBSBufferDataset_t.py,v 1.2 2009/10/13 20:55:27 meloam Exp $"
__version__ = "$Revision: 1.2 $"

import unittest
import threading

from WMCore.DAOFactory import DAOFactory
from WMQuality.TestInit import TestInit

class DBSBufferDatasetTest(unittest.TestCase):

    
    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        DBSBuffer tables.
        """

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMComponent.DBSBuffer.Database"],
                                useDefault = False)

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)


          
    def tearDown(self):        
        """
        _tearDown_
        
        Drop all the DBSBuffer tables.
        """
        self.testInit.clearDatabase()

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
