#!/usr/bin/env python
"""
_DBSBufferAlgoDatasetAssoc_t_

Unit tests for manipulating associating algorithms to datasets in DBSBuffer.
"""

__revision__ = "$Id: DBSBufferAlgoDatasetAssoc_t.py,v 1.2 2009/10/13 20:53:41 meloam Exp $"
__version__ = "$Revision: 1.2 $"

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

        datasetAction = self.daoFactory(classname = "NewDataset")
        datasetListAction = self.daoFactory(classname = "ListDataset")
        datasetAction.execute(datasetPath = "/Cosmics/CRUZET09-PromptReco-v1/RECO")

        listResult = datasetListAction.execute(datasetPath = "/Cosmics/CRUZET09-PromptReco-v1/RECO")
        self.datasetID = listResult[0]["id"]

        algoAction = self.daoFactory(classname = "NewAlgo")
        algoListAction = self.daoFactory(classname = "ListAlgo")
        algoAction.execute(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                           appFam = "FEVT", psetHash = "GIBBERISH",
                           configContent = "MOREGIBBERISH")
        algoAction.execute(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                           appFam = "FEVT", psetHash = "GIBBERISH2",
                           configContent = "MOREGIBBERISH2")

        listResult = algoListAction.execute(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                                            appFam = "FEVT", psetHash = "GIBBERISH",
                                            configContent = "MOREGIBBERISH")
        self.algo1ID = listResult[0]["id"]

        listResult = algoListAction.execute(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                                            appFam = "FEVT", psetHash = "GIBBERISH2",
                                            configContent = "MOREGIBBERISH2")
        self.algo2ID = listResult[0]["id"]

          
    def tearDown(self):        
        """
        _tearDown_
        
        Drop all the DBSBuffer tables.
        """
        self.testInit.clearDatabase()

            
    def testCreate(self):
        """
        _testCreate_

        Verify that the association of an algorithm to a dataset in the
        DBSBuffer works correctly.
        """
        newAssocAction = self.daoFactory(classname = "AlgoDatasetAssoc")
        listAssocAction = self.daoFactory(classname = "ListAlgoDatasetAssoc")

        assocID = newAssocAction.execute(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                                         appFam = "FEVT", psetHash = "GIBBERISH",
                                         datasetPath = "/Cosmics/CRUZET09-PromptReco-v1/RECO")

        result = listAssocAction.execute(assocID = assocID)

        assert len(result) == 1, \
               "ERROR: One association should be returned."
        assert result[0]["algo_id"] == self.algo1ID, \
               "ERROR: Wrong algorithm associated with dataset"
        assert result[0]["dataset_id"] == self.datasetID, \
               "ERROR: Wrong dataset associated with algorithm"
        assert result[0]["in_dbs"] == 0, \
               "ERROR: Dataset/Algo association shouldn't be in DBS"

        assocID = newAssocAction.execute(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                                         appFam = "FEVT", psetHash = "GIBBERISH2",
                                         datasetPath = "/Cosmics/CRUZET09-PromptReco-v1/RECO")

        result = listAssocAction.execute(assocID = assocID)

        assert len(result) == 1, \
               "ERROR: One association should be returned."
        assert result[0]["algo_id"] == self.algo2ID, \
               "ERROR: Wrong algorithm associated with dataset"
        assert result[0]["dataset_id"] == self.datasetID, \
               "ERROR: Wrong dataset associated with algorithm"
        assert result[0]["in_dbs"] == 0, \
               "ERROR: Dataset/Algo association shouldn't be in DBS"

        assocID = newAssocAction.execute(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                                         appFam = "FEVT", psetHash = "GIBBERISH2",
                                         datasetPath = "/Cosmics/CRUZET09-PromptReco-v1/RECO")

        result = listAssocAction.execute(assocID = assocID)

        assert result[0]["algo_id"] == self.algo2ID, \
               "ERROR: Wrong algorithm associated with dataset"
        assert result[0]["dataset_id"] == self.datasetID, \
               "ERROR: Wrong dataset associated with algorithm"
        assert result[0]["in_dbs"] == 0, \
               "ERROR: Dataset/Algo association shouldn't be in DBS"
        return

    def testCreateTransaction(self):
        """
        _testCreateTransaction_

        Verify that the new dataset DAO object handles transactions correctly.
        """
        myThread = threading.currentThread()
        myThread.transaction.begin()

        newAssocAction = self.daoFactory(classname = "AlgoDatasetAssoc")
        listAssocAction = self.daoFactory(classname = "ListAlgoDatasetAssoc")

        assocID = newAssocAction.execute(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                                         appFam = "FEVT", psetHash = "GIBBERISH",
                                         datasetPath = "/Cosmics/CRUZET09-PromptReco-v1/RECO",
                                         conn = myThread.transaction.conn, transaction = True)

        result = listAssocAction.execute(assocID = assocID, conn = myThread.transaction.conn,
                                         transaction = True)

        assert result[0]["algo_id"] == self.algo1ID, \
               "ERROR: Wrong algorithm associated with dataset"
        assert result[0]["dataset_id"] == self.datasetID, \
               "ERROR: Wrong dataset associated with algorithm"
        assert result[0]["in_dbs"] == 0, \
               "ERROR: Dataset/Algo association shouldn't be in DBS"

        myThread.transaction.rollback()

        result = listAssocAction.execute(assocID = assocID)

        assert len(result) == 0, \
               "ERROR: Transaction did not roll back properly."

        return

    def testUpdate(self):
        """
        _testUpdate_

        Verify that the UpdateAlgoDatasetAssoc DAO object works correctly.
        """
        newAssocAction = self.daoFactory(classname = "AlgoDatasetAssoc")
        listAssocAction = self.daoFactory(classname = "ListAlgoDatasetAssoc")
        updateAssocAction = self.daoFactory(classname = "UpdateAlgoDatasetAssoc")

        assocID = newAssocAction.execute(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                                         appFam = "FEVT", psetHash = "GIBBERISH",
                                         datasetPath = "/Cosmics/CRUZET09-PromptReco-v1/RECO")

        result = listAssocAction.execute(assocID = assocID)

        assert len(result) == 1, \
               "ERROR: One association should be returned."
        assert result[0]["algo_id"] == self.algo1ID, \
               "ERROR: Wrong algorithm associated with dataset"
        assert result[0]["dataset_id"] == self.datasetID, \
               "ERROR: Wrong dataset associated with algorithm"
        assert result[0]["in_dbs"] == 0, \
               "ERROR: Dataset/Algo association shouldn't be in DBS"

        updateAssocAction.execute(assocID = assocID, inDBS = 1)

        result = listAssocAction.execute(assocID = assocID)

        assert len(result) == 1, \
               "ERROR: One association should be returned."
        assert result[0]["algo_id"] == self.algo1ID, \
               "ERROR: Wrong algorithm associated with dataset"
        assert result[0]["dataset_id"] == self.datasetID, \
               "ERROR: Wrong dataset associated with algorithm"
        assert result[0]["in_dbs"] == 1, \
               "ERROR: Dataset/Algo association should be in DBS"         

        updateAssocAction.execute(assocID = assocID, inDBS = 0)

        result = listAssocAction.execute(assocID = assocID)

        assert len(result) == 1, \
               "ERROR: One association should be returned."
        assert result[0]["algo_id"] == self.algo1ID, \
               "ERROR: Wrong algorithm associated with dataset"
        assert result[0]["dataset_id"] == self.datasetID, \
               "ERROR: Wrong dataset associated with algorithm"
        assert result[0]["in_dbs"] == 0, \
               "ERROR: Dataset/Algo association shouldn't be in DBS"         

        return

    def testUpdateTransaction(self):
        """
        _testUpdateTransaction_

        """
        newAssocAction = self.daoFactory(classname = "AlgoDatasetAssoc")
        listAssocAction = self.daoFactory(classname = "ListAlgoDatasetAssoc")
        updateAssocAction = self.daoFactory(classname = "UpdateAlgoDatasetAssoc")

        assocID = newAssocAction.execute(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                                         appFam = "FEVT", psetHash = "GIBBERISH",
                                         datasetPath = "/Cosmics/CRUZET09-PromptReco-v1/RECO")

        result = listAssocAction.execute(assocID = assocID)

        assert len(result) == 1, \
               "ERROR: One association should be returned."
        assert result[0]["algo_id"] == self.algo1ID, \
               "ERROR: Wrong algorithm associated with dataset"
        assert result[0]["dataset_id"] == self.datasetID, \
               "ERROR: Wrong dataset associated with algorithm"
        assert result[0]["in_dbs"] == 0, \
               "ERROR: Dataset/Algo association shouldn't be in DBS"

        myThread = threading.currentThread()
        myThread.transaction.begin()

        updateAssocAction.execute(assocID = assocID, inDBS = 1,
                                  conn = myThread.transaction.conn,
                                  transaction = True)

        result = listAssocAction.execute(assocID = assocID,
                                         conn = myThread.transaction.conn,
                                         transaction = True)

        assert len(result) == 1, \
               "ERROR: One association should be returned."
        assert result[0]["algo_id"] == self.algo1ID, \
               "ERROR: Wrong algorithm associated with dataset"
        assert result[0]["dataset_id"] == self.datasetID, \
               "ERROR: Wrong dataset associated with algorithm"
        assert result[0]["in_dbs"] == 1, \
               "ERROR: Dataset/Algo association should be in DBS"

        myThread.transaction.rollback()

        result = listAssocAction.execute(assocID = assocID)

        assert result[0]["in_dbs"] == 0, \
               "ERROR: Dataset/Algo association should be in DBS"        

        return

if __name__ == "__main__":
    unittest.main() 
