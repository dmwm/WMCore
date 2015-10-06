#!/usr/bin/env python
"""
_addToBuffer_

APIs related to adding file to DBS Buffer

"""




import logging
import os
import threading
from WMCore.WMFactory import WMFactory
from WMCore.DAOFactory import DAOFactory
from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile
from WMCore.DataStructs.Run import Run
from WMCore.WMConnectionBase import WMConnectionBase

class AddToBuffer(WMConnectionBase):
    """
    APIs related to file addition for DBSBuffer

    """

    def __init__(self, logger=None, dbfactory = None):
        pass

    def addFile(self, file, dataset=0):
        """
        Add the file to the buffer
        """

        myThread = threading.currentThread()

        existingTransaction = self.beginTransaction()

        bufferFile = DBSBufferFile(lfn = file['LFN'], size = file['Size'], events = file['TotalEvents'],
                                cksum=file['Checksum'], dataset=dataset)

        runLumiList=file.getLumiSections()
        runList=[x['RunNumber'] for x in runLumiList]

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
        # Add the dataset to the buffer (API Call)
        myThread = threading.currentThread()

        existingTransaction = self.beginTransaction()

        factory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                             logger = myThread.logger,
                             dbinterface = myThread.dbi)
        newDS = factory(classname = "NewDataset")
        newDS.execute(datasetPath=dataset["Path"], conn = self.getDBConn(), transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return

    def addAlgo(self, algo):
        # Add the algo to the buffer (API Call)
        # dataset object contains the algo information
        myThread = threading.currentThread()

        existingTransaction = self.beginTransaction()

        factory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        newDS = factory(classname = "NewAlgo")
        newDS.execute(appName = algo["ApplicationName"], appVer = algo["ApplicationVersion"], appFam = algo["ApplicationName"], \
                      psetHash = algo["PSetHash"], configContent = algo["PSetContent"], \
                      conn = self.getDBConn(), transaction=self.existingTransaction())
        self.commitTransaction(existingTransaction)
        return

    def updateDSFileCount(self, dataset):
        myThread = threading.currentThread()

        existingTransaction = self.beginTransaction()

        factory = WMFactory("dbsBuffer", "WMComponent.DBSBuffer.Database."+ \
                        myThread.dialect)
        newDS = factory.loadObject("UpdateDSFileCount")
        newDS.execute(dataset=dataset, conn = self.getDBConn(), transaction=self.existingTransaction())
        self.commitTransaction(existingTransaction)
        return


    def updateAlgo(self, algo, inDBS = 0):
        #Update the algo with inDBS information
        myThread = threading.currentThread()

        existingTransaction = self.beginTransaction()

        factory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                             logger = myThread.logger,
                             dbinterface = myThread.dbi)
        newDS = factory(classname = "UpdateAlgo")
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

        factory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                             logger = myThread.logger,
                             dbinterface = myThread.dbi)

        binds = []
        for f in fileObjs:
            binds.append(f["id"])


        loadFiles = factory(classname = "DBSBufferFiles.LoadBulkFilesByID")
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
