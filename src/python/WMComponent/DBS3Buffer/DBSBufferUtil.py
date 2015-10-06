#!/usr/bin/env python
"""
_DBSBufferUtil_

APIs related to using the DBSBuffer.
"""

import logging
import threading

from WMCore.WMFactory import WMFactory
from WMCore.DAOFactory import DAOFactory
from WMComponent.DBS3Buffer.DBSBufferFile import DBSBufferFile
from WMCore.DataStructs.Run import Run
from WMCore.WMConnectionBase import WMConnectionBase

class DBSBufferUtil(WMConnectionBase):
    """
    APIs related to file addition for DBSBuffer

    """

    def __init__(self, logger=None, dbfactory = None):

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package = "WMComponent.DBS3Buffer",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)


    def addFile(self, file, dataset=0):
        """
        Add the file to the buffer
        """

        myThread = threading.currentThread()

        existingTransaction = self.beginTransaction()

        bufferFile = DBSBufferFile(lfn = file['LFN'], size = file['Size'],
                                   events = file['TotalEvents'],
                                   cksum=file['Checksum'], dataset=dataset)

        runLumiList = file.getLumiSections()
        runList     = [x['RunNumber'] for x in runLumiList]

        for runNumber in runList:
            lumis = [int(y['LumiSectionNumber']) for y in runLumiList if y['RunNumber']==runNumber]
            run=Run(runNumber, *lumis)
            bufferFile.addRun(run)

        if bufferFile.exists() == False:
            bufferFile.create()
            bufferFile.setLocation(pnn=file['locations'], immediateSave = True)
        else:
            bufferFile.load()
        # Lets add the file to DBS Buffer as well
        #UPDATE File Count

        self.updateDSFileCount(dataset=dataset)

        #Parent files
        bufferFile.addParents(file.inputFiles)

        self.commitTransaction(existingTransaction)

        return


    def addDataset(self, dataset, algoInDBS):
        """
        Add the dataset to the buffer (API Call)
        """
        myThread = threading.currentThread()

        existingTransaction = self.beginTransaction()

        newDS = self.daoFactory(classname = "NewDataset")
        newDS.execute(datasetPath=dataset["Path"], conn = self.getDBConn(), transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return

    def addAlgo(self, algo):
        """
        Add the algo to the buffer (API Call)
        dataset object contains the algo information
        """
        myThread = threading.currentThread()

        existingTransaction = self.beginTransaction()

        newDS = self.daoFactory(classname = "NewAlgo")
        newDS.execute(appName = algo["ApplicationName"], appVer = algo["ApplicationVersion"], appFam = algo["ApplicationName"], \
                      psetHash = algo["PSetHash"], configContent = algo["PSetContent"], \
                      conn = self.getDBConn(), transaction=self.existingTransaction())
        self.commitTransaction(existingTransaction)
        return

    def updateDSFileCount(self, dataset):
        """
        _updateDSFileCount_

        Update a dataset with its files
        """

        existingTransaction = self.beginTransaction()

        newDS = self.daoFactory(classname = "UpdateDSFileCount")
        newDS.execute(dataset=dataset, conn = self.getDBConn(), transaction=self.existingTransaction())
        self.commitTransaction(existingTransaction)
        return


    def updateAlgo(self, algo, inDBS = 0):
        """
        Update the algo with inDBS information
        """
        myThread = threading.currentThread()

        existingTransaction = self.beginTransaction()

        newDS = self.daoFactory(classname = "UpdateAlgo")
        newDS.execute(inDBS = inDBS, appName = algo["ApplicationName"], appVer = algo["ApplicationVersion"], appFam = algo["ApplicationFamily"], \
                      psetHash = algo["PSetHash"], conn = self.getDBConn(), transaction=self.existingTransaction())
        self.commitTransaction(existingTransaction)
        return


    def loadDBSBufferFilesBulk(self, fileObjs):
        """
        _loadDBSBufferFilesBulk_

        Yes, this is a stupid place to put it.
        No, there's not better place.
        """


        myThread = threading.currentThread()

        dbsFiles = []

        existingTransaction = self.beginTransaction()

        binds = []
        for f in fileObjs:
            binds.append(f["id"])


        loadFiles = self.daoFactory(classname = "DBSBufferFiles.LoadBulkFilesByID")
        results = loadFiles.execute(files = binds, conn = self.getDBConn(),
                                    transaction = self.existingTransaction())


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



    def findUploadableDAS(self):
        """
        _findUploadableDAS_

        Find all the Dataset-Algo combinations
        with uploadable files.
        """

        myThread = threading.currentThread()
        existingTransaction = self.beginTransaction()

        findDAS = self.daoFactory(classname = "FindDASToUpload")
        result  = findDAS.execute(conn = self.getDBConn(),
                                  transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)

        return result




    def findOpenBlocks(self):
        """
        _findOpenBlocks_

        This should find all blocks.
        """
        myThread = threading.currentThread()
        existingTransaction = self.beginTransaction()

        openBlocks = self.daoFactory(classname = "GetOpenBlocks")
        result = openBlocks.execute(conn = self.getDBConn(),
                                    transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return result


    def loadBlocksByDAS(self, das):
        """
        _loadBlocksByDAS_

        Given a DAS, find all the
        blocks associated with it in the
        Open status
        """

        myThread = threading.currentThread()
        existingTransaction = self.beginTransaction()

        findBlocks = self.daoFactory(classname = "LoadBlocksByDAS")
        result     = findBlocks.execute(das = das,
                                        conn = self.getDBConn(),
                                        transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)

        return result




    def loadBlocks(self, blocknames):
        """
        _loadBlocks_

        Given a list of names, load the
        blocks with those names
        """

        if len(blocknames) < 1:
            # Nothing to do
            return []

        myThread = threading.currentThread()
        existingTransaction = self.beginTransaction()

        findBlocks = self.daoFactory(classname = "LoadBlocks")
        result     = findBlocks.execute(blocknames,
                                        conn = self.getDBConn(),
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

        findFiles = self.daoFactory(classname = "LoadDBSFilesByDAS")
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


    def updateBlocks(self, blocks):
        """
        _updateBlocks_

        Update the blocks in DBSBuffer
        """
        if len(blocks) < 1:
            # Nothing to do
            return

        myThread = threading.currentThread()
        existingTransaction = self.beginTransaction()

        updateBlock = self.daoFactory(classname = "UpdateBlocks")
        updateBlock.execute(blocks, conn = self.getDBConn(),
                            transaction=self.existingTransaction())
        self.commitTransaction(existingTransaction)
        return

    def updateFileStatus(self, blocks, status):
        """
        _updateFileStatus_

        Update the status of files that are associated with the given block.
        """
        if len(blocks) < 1:
            return

        myThread = threading.currentThread()
        existingTransaction = self.beginTransaction()

        updateBlock = self.daoFactory(classname = "UpdateFiles")
        updateBlock.execute(blocks, status, conn = self.getDBConn(),
                            transaction = self.existingTransaction())
        self.commitTransaction(existingTransaction)
        return

    def createBlocks(self, blocks):
        """
        _createBlocks_

        Create blocks in the DBSBuffer
        """

        if len(blocks) < 1:
            return

        existingTransaction = self.beginTransaction()

        newBlocks = self.daoFactory(classname = "CreateBlocks")
        newBlocks.execute(blocks = blocks, conn = self.getDBConn(),
                          transaction=self.existingTransaction())
        self.commitTransaction(existingTransaction)
        return


    def setBlockFiles(self, binds):
        """
        _SetBlockFiles_

        Set files to have the right blocks/
        """

        if len(binds) < 1:
            # Nothing to do
            return

        existingTransaction = self.beginTransaction()

        setBlocks = self.daoFactory(classname = "SetBlockFiles")
        setBlocks.execute(binds = binds, conn = self.getDBConn(),
                          transaction=self.existingTransaction())
        self.commitTransaction(existingTransaction)
        return


    def loadFilesByBlock(self, blockname):
        """
        _loadFilesByBlock_

        Get all files associated with a block
        """

        dbsFiles = []

        existingTransaction = self.beginTransaction()

        findFiles = self.daoFactory(classname = "LoadFilesByBlock")
        results   = findFiles.execute(blockname = blockname,
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
    
    
    def getCompletedWorkflows(self):
        """
        _getCompletedWorkflows_

        """

        myThread = threading.currentThread()
        existingTransaction = self.beginTransaction()

        wfCompletedDAO = self.daoFactory(classname = "GetCompletedWorkflows")
        result  = wfCompletedDAO.execute(conn = self.getDBConn(),
                                            transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)

        return result
