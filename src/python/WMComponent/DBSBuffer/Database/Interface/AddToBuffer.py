#!/usr/bin/env python
"""
_addToBuffer_

APIs related to adding file to DBS Buffer

"""
__version__ = "$Revision: 1.14 $"
__revision__ = "$Id: AddToBuffer.py,v 1.14 2009/09/03 19:31:31 mnorman Exp $"


import logging
import os
import threading
from WMCore.WMFactory import WMFactory
from WMCore.DAOFactory import DAOFactory
from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile
from WMCore.DataStructs.Run import Run

class AddToBuffer:
    def addFile(self, file, dataset=0):

        myThread = threading.currentThread()
        myThread.transaction.begin()

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
            bufferFile.setLocation(se=file['SEName'], immediateSave = True)
	else:
            bufferFile.load()
	# Lets add the file to DBS Buffer as well
        #UPDATE File Count

	self.updateDSFileCount(dataset=dataset)

	#Parent files
        for inputFile in file.inputFiles:
		#Parent file in this case doesn't get added to DBSBuffer
		parentFile = DBSBufferFile(lfn=inputFile['LFN'])

		if parentFile.exists() == False: 
			parentFile.create() 
		else: 
			parentFile.load()
		try:
			bufferFile.addParent(parentFile['lfn'])
		except Exception, ex:
                        if ex.__str__().find("Duplicate entry") != -1 :
                                pass
                        else:
                                raise ex

        myThread.transaction.commit()
        return


    def addDataset(self, dataset, algoInDBS):
        # Add the dataset to the buffer (API Call)
        myThread = threading.currentThread()
        myThread.transaction.begin()
        
        factory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                             logger = myThread.logger,
                             dbinterface = myThread.dbi)
        newDS = factory(classname = "NewDataset")
        newDS.execute(datasetPath=dataset["Path"], conn = myThread.transaction.conn, transaction=myThread.transaction)
        myThread.transaction.commit()
        return
    
    def addAlgo(self, algo):
        # Add the algo to the buffer (API Call)
        # dataset object contains the algo information
        myThread = threading.currentThread()
        myThread.transaction.begin()

        factory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        newDS = factory(classname = "NewAlgo")
        newDS.execute(appName = algo["ApplicationName"], appVer = algo["ApplicationVersion"], appFam = algo["ApplicationName"], \
                      psetHash = algo["PSetHash"], configContent = algo["PSetContent"], \
                      conn = myThread.transaction.conn, transaction=myThread.transaction)
        myThread.transaction.commit()
        return

    def updateDSFileCount(self, dataset):
	myThread = threading.currentThread()
        myThread.transaction.begin()

        factory = WMFactory("dbsBuffer", "WMComponent.DBSBuffer.Database."+ \
                        myThread.dialect)
        newDS = factory.loadObject("UpdateDSFileCount")
        newDS.execute(dataset=dataset, conn = myThread.transaction.conn, transaction=myThread.transaction)
        myThread.transaction.commit()
        return


    def updateAlgo(self, algo, inDBS = 0):
        #Update the algo with inDBS information
        myThread = threading.currentThread()
        myThread.transaction.begin()

        factory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                             logger = myThread.logger,
                             dbinterface = myThread.dbi)
        newDS = factory(classname = "UpdateAlgo")
        newDS.execute(inDBS = inDBS, appName = algo["ApplicationName"], appVer = algo["ApplicationVersion"], appFam = algo["ApplicationFamily"], \
                      psetHash = algo["PSetHash"], conn = myThread.transaction.conn, transaction=myThread.transaction)
        myThread.transaction.commit()
        return
    
