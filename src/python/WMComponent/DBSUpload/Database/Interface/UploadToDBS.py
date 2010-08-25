#!/usr/bin/env python
"""
_UploadToDBS_

APIs related to adding file to DBS

"""



import logging
import threading
from WMCore.WMFactory        import WMFactory
from WMCore.DAOFactory       import DAOFactory
from WMCore.WMConnectionBase import WMConnectionBase

from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile
from WMCore.DataStructs.Run import Run


class UploadToDBS (WMConnectionBase):
    """
    APIs relating to adding files, etc. to DBS

    """

    def __init__(self, logger=None, dbfactory = None):
        pass
    

    def findAlgos(self, dataset):
        """
        Call to findAlgos

        """
        myThread = threading.currentThread()
        existingTransaction = self.beginTransaction()
        
        factory = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database."+ \
                        myThread.dialect)
        findAlgos = factory.loadObject("FindAlgos")
        # Add the file to the buffer (API Call)
        
        #results = findFiles.execute(conn = self.getDBConn(), transaction=self.existingTransaction())
        results = findAlgos.execute(datasetInfo=dataset, conn = self.getDBConn(), transaction=self.existingTransaction())
        self.commitTransaction(existingTransaction)
        return results  

    def updateFilesStatus(self, files, status):
        """
        _updateFileStatus_

        Update the status of a series of files in DBSBuffer.  The files must be
        passed in as a list of LFNs.
        """

        if len(files) == 0:
            return
        
        myThread = threading.currentThread()
        existingTransaction = self.beginTransaction()
        
        factory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                             logger = myThread.logger,
                             dbinterface = myThread.dbi)
        statusAction = factory(classname = "DBSBufferFiles.SetStatus")
        statusAction.execute(lfns = files, status = status,
                             conn = self.getDBConn(),
                             transaction=self.existingTransaction())
        self.commitTransaction(existingTransaction)
        return
    
    def setBlockStatus(self, block, locations, openStatus = 0, time = 0):
        """
        _setBlockStatus_

        Adds information about the block from DBSWriter to the database
        """
        myThread = threading.currentThread()
        existingTransaction = self.beginTransaction()
        
        factory = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database."+ \
                        myThread.dialect)
        newDS = factory.loadObject("SetBlockStatus")
        newDS.execute(block = block, locations = locations, open_status = openStatus, time = time, \
                      conn = self.getDBConn(), transaction=self.existingTransaction())
        self.commitTransaction(existingTransaction)
        return

    def findOpenBlocks(self):
        """
        _findOpenBlocks_
        
        This should find all blocks.
        """

        myThread = threading.currentThread()
        existingTransaction = self.beginTransaction()
        
        factory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                             logger = myThread.logger,
                             dbinterface = myThread.dbi)
        openBlocks = factory(classname = "GetOpenBlocks")

        result = openBlocks.execute(conn = self.getDBConn(), transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return result


    def setDatasetAlgo(self, datasetAlgoInfo, inDBS = 1):
        """
        _setDatasetAlgo_

        Sets the datasetAlgo status to uploaded in_dbs
        """
        myThread = threading.currentThread()
        existingTransaction = self.beginTransaction()

        factory = DAOFactory(package = "WMComponent.DBSUpload.Database",
                             logger = myThread.logger,
                             dbinterface = myThread.dbi)
        setDA = factory(classname = "SetDatasetAlgo")
        setDA.execute(datasetAlgo = datasetAlgoInfo['DAS_ID'], inDBS = inDBS,
                      conn = self.getDBConn(),
                      transaction=self.existingTransaction())


        self.commitTransaction(existingTransaction)

        return



    def findUploadableDAS(self):
        """
        _findUploadableDAS_

        Find all the Dataset-Algo combinations
        with uploadable files.
        """

        myThread = threading.currentThread()
        existingTransaction = self.beginTransaction()

        factory = DAOFactory(package = "WMComponent.DBSUpload.Database",
                             logger = myThread.logger,
                             dbinterface = myThread.dbi)
        findDAS = factory(classname = "FindDASToUpload")
        result  = findDAS.execute(conn = self.getDBConn(),
                                  transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)

        return result



    def findUploadableFilesByDAS(self, das):
        """
        _findUploadableDAS_

        Find all the Dataset-Algo files available
        with uploadable files.
        """

        myThread = threading.currentThread()
        existingTransaction = self.beginTransaction()

        dbsFiles = []

        factory = DAOFactory(package = "WMComponent.DBSUpload.Database",
                             logger = myThread.logger,
                             dbinterface = myThread.dbi)
        findFiles = factory(classname = "LoadDBSFilesByDAS")
        results   = findFiles.execute(das = das,
                                      conn = self.getDBConn(),
                                      transaction=self.existingTransaction())

        for entry in results:
            # Add loaded information
            dbsfile = DBSBufferFile(id=entry['id'])
            dbsfile.update(entry)
            dbsFiles.append(dbsfile)

        for dbsfile in dbsFiles:
            if 'runInfo' in dbsfile.keys():
                # Then we have to replace it with a real run
                for r in dbsfile['runInfo'].keys():
                    run = Run(runNumber = r)
                    run.extend(dbsfile['runInfo'][r])
                    dbsfile.addRun(run)
                del dbsfile['runInfo']
            if 'parentLFNs' in dbsfile.keys():
                # Then we have some parents
                for lfn in dbsfile['parentLFNs']:
                    newFile = DBSBufferFile(lfn = lfn)
                    dbsfile['parents'].add(newFile)
                del dbsfile['parentLFNs']

        self.commitTransaction(existingTransaction)

        return dbsFiles


    def loadBlocksByDAS(self, das):
        """
        _loadBlocksByDAS_

        Given a DAS, find all the 
        blocks associated with it in the
        Open status
        """

        myThread = threading.currentThread()
        existingTransaction = self.beginTransaction()

        factory = DAOFactory(package = "WMComponent.DBSUpload.Database",
                             logger = myThread.logger,
                             dbinterface = myThread.dbi)
        findBlocks = factory(classname = "LoadBlocksByDAS")
        result     = findBlocks.execute(das = das,
                                        conn = self.getDBConn(),
                                        transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)

        return result
        
