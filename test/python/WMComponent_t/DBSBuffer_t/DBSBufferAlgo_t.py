#!/usr/bin/env python
"""
_DBSBufferAlgo_t_

Unit tests for manipulating algorithm in DBSBuffer.
"""

__revision__ = "$Id: DBSBufferAlgo_t.py,v 1.1 2009/06/19 15:37:37 sfoulkes Exp $"
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

        Verify that the creation of an algorithm in the DBSBuffer works
        correctly.
        """
        newAlgoAction = self.daoFactory(classname = "NewAlgo")
        listAlgoAction = self.daoFactory(classname = "ListAlgo")
        
        newAlgoAction.execute(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                              appFam = "FEVT", psetHash = "GIBBERISH",
                              configContent = "MOREGIBBERISH")
        
        resultA = listAlgoAction.execute(appName = "cmsRun",
                                            appVer = "CMSSW_2_1_8",
                                            appFam = "FEVT",
                                            psetHash = "GIBBERISH",
                                            configContent = "MOREGIBBERISH")

        assert len(resultA) == 1, \
               "ERROR: Wrong number of algos returned: %s" % len(resultA)
        assert resultA[0]["app_name"] == "cmsRun", \
               "ERROR: AppName is wrong."
        assert resultA[0]["app_ver"] == "CMSSW_2_1_8", \
               "ERROR: AppVer is wrong."
        assert resultA[0]["app_fam"] == "FEVT", \
               "ERROR: AppFam is wrong."
        assert resultA[0]["pset_hash"] == "GIBBERISH", \
               "ERROR: PSetHash is wrong."
        assert resultA[0]["config_content"] == "MOREGIBBERISH", \
               "ERROR: ConfigContent is wrong."
        assert resultA[0]["in_dbs"] == 0, \
               "ERROR: Algo should not be marked as in DBS"

        newAlgoAction.execute(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                              appFam = "FEVT", psetHash = "GIBBERISH",
                              configContent = "MOREGIBBERISH")
        resultB = listAlgoAction.execute(algoID = resultA[0]["id"])

        assert len(resultB) == 1, \
               "ERROR: Wrong number of algos returned: %s" % len(resultA)
        assert resultB[0]["app_name"] == "cmsRun", \
               "ERROR: AppName is wrong."
        assert resultB[0]["app_ver"] == "CMSSW_2_1_8", \
               "ERROR: AppVer is wrong."
        assert resultB[0]["app_fam"] == "FEVT", \
               "ERROR: AppFam is wrong."
        assert resultB[0]["pset_hash"] == "GIBBERISH", \
               "ERROR: PSetHash is wrong."
        assert resultB[0]["config_content"] == "MOREGIBBERISH", \
               "ERROR: ConfigContent is wrong."
        assert resultB[0]["in_dbs"] == 0, \
               "ERROR: Algo should not be marked as in DBS"

        return

    def testCreateTransaction(self):
        """
        _testCreateTransaction_

        Verify that the new dataset DAO object handles transactions correctly.
        """
        myThread = threading.currentThread()
        myThread.transaction.begin()

        newAlgoAction = self.daoFactory(classname = "NewAlgo")
        listAlgoAction = self.daoFactory(classname = "ListAlgo")

        newAlgoAction.execute(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                              appFam = "FEVT", psetHash = "GIBBERISH",
                              configContent = "MOREGIBBERISH")
        
        resultA = listAlgoAction.execute(appName = "cmsRun",
                                            appVer = "CMSSW_2_1_8",
                                            appFam = "FEVT",
                                            psetHash = "GIBBERISH",
                                            configContent = "MOREGIBBERISH")

        assert len(resultA) == 1, \
               "ERROR: Wrong number of algos returned: %s" % len(resultA)
        assert resultA[0]["app_name"] == "cmsRun", \
               "ERROR: AppName is wrong."
        assert resultA[0]["app_ver"] == "CMSSW_2_1_8", \
               "ERROR: AppVer is wrong."
        assert resultA[0]["app_fam"] == "FEVT", \
               "ERROR: AppFam is wrong."
        assert resultA[0]["pset_hash"] == "GIBBERISH", \
               "ERROR: PSetHash is wrong."
        assert resultA[0]["config_content"] == "MOREGIBBERISH", \
               "ERROR: ConfigContent is wrong."
        assert resultA[0]["in_dbs"] == 0, \
               "ERROR: Algo should not be marked as in DBS"
        
        myThread.transaction.rollback()

        resultB = listAlgoAction.execute(algoID = resultA[0]["id"])

        assert len(resultB) == 1, \
               "ERROR: Transaction did not roll back properly."

        return

    def testUpdate(self):
        """
        _testUpdate_

        Verify that the UpdateAlgo DAO object works as expected.
        """
        newAlgoAction = self.daoFactory(classname = "NewAlgo")
        listAlgoAction = self.daoFactory(classname = "ListAlgo")
        updateAlgoAction = self.daoFactory(classname = "UpdateAlgo")        
        
        newAlgoAction.execute(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                              appFam = "FEVT", psetHash = "GIBBERISH",
                              configContent = "MOREGIBBERISH")
        
        resultA = listAlgoAction.execute(appName = "cmsRun",
                                            appVer = "CMSSW_2_1_8",
                                            appFam = "FEVT",
                                            psetHash = "GIBBERISH",
                                            configContent = "MOREGIBBERISH")

        assert len(resultA) == 1, \
               "ERROR: Wrong number of algos returned: %s" % len(resultA)
        assert resultA[0]["in_dbs"] == 0, \
               "ERROR: Algo should not be marked as in DBS"
        
        updateAlgoAction.execute(inDBS = 1, algoID = resultA[0]["id"])

        resultB = listAlgoAction.execute(appName = "cmsRun",
                                            appVer = "CMSSW_2_1_8",
                                            appFam = "FEVT",
                                            psetHash = "GIBBERISH",
                                            configContent = "MOREGIBBERISH")

        assert len(resultB) == 1, \
               "ERROR: Wrong number of algos returned: %s" % len(resultA)
        assert resultB[0]["in_dbs"] == 1, \
               "ERROR: Algo should not be marked as in DBS"

        return

    def testUpdateTransaction(self):
        """
        _testUpdateTransaction_

        Verify that the UpdateAlgo DAO object uses transactions correctly.
        """
        newAlgoAction = self.daoFactory(classname = "NewAlgo")
        listAlgoAction = self.daoFactory(classname = "ListAlgo")
        updateAlgoAction = self.daoFactory(classname = "UpdateAlgo")        
        
        newAlgoAction.execute(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                              appFam = "FEVT", psetHash = "GIBBERISH",
                              configContent = "MOREGIBBERISH")
        
        resultA = listAlgoAction.execute(appName = "cmsRun",
                                            appVer = "CMSSW_2_1_8",
                                            appFam = "FEVT",
                                            psetHash = "GIBBERISH",
                                            configContent = "MOREGIBBERISH")

        assert len(resultA) == 1, \
               "ERROR: Wrong number of algos returned: %s" % len(resultA)
        assert resultA[0]["in_dbs"] == 0, \
               "ERROR: Algo should not be marked as in DBS"

        myThread = threading.currentThread()
        myThread.transaction.begin()
        
        updateAlgoAction.execute(inDBS = 1, algoID = resultA[0]["id"],
                                 conn = myThread.transaction.conn,
                                 transaction = True)

        resultB = listAlgoAction.execute(appName = "cmsRun",
                                         appVer = "CMSSW_2_1_8",
                                         appFam = "FEVT",
                                         psetHash = "GIBBERISH",
                                         configContent = "MOREGIBBERISH",
                                         conn = myThread.transaction.conn,
                                         transaction = True)                             

        assert len(resultB) == 1, \
               "ERROR: Wrong number of algos returned: %s" % len(resultA)
        assert resultB[0]["in_dbs"] == 1, \
               "ERROR: Algo should be marked as in DBS"

        myThread.transaction.rollback()

        resultC = listAlgoAction.execute(appName = "cmsRun",
                                         appVer = "CMSSW_2_1_8",
                                         appFam = "FEVT",
                                         psetHash = "GIBBERISH",
                                         configContent = "MOREGIBBERISH")
        assert len(resultC) == 1, \
               "ERROR: Wrong number of algos returned: %s" % len(resultA)
        assert resultC[0]["in_dbs"] == 0, \
               "ERROR: Algo should not be marked as in DBS"        

        return    

if __name__ == "__main__":
    unittest.main() 
