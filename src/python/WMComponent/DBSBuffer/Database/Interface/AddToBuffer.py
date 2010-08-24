#!/usr/bin/env python
"""
_addToBuffer_

APIs related to adding file to DBS Buffer

"""
__version__ = "$Revision: 1.8 $"
__revision__ = "$Id: AddToBuffer.py,v 1.8 2008/12/23 20:34:02 afaq Exp $"
__author__ = "anzar@fnal.gov"

import logging
import threading
from WMCore.WMFactory import WMFactory
from WMCore.WMBS.File import File
from WMCore.DataStructs.Run import Run

class AddToBuffer:

    def __init__(self, logger=None, dbfactory = None):
        pass
    
    def addFile(self, file, dataset=0):
        myThread = threading.currentThread()
        myThread.transaction.begin()
        factory = WMFactory("dbsBuffer", "WMComponent.DBSBuffer.Database."+ \
                        myThread.dialect)
        newFile = factory.loadObject("NewFile")
        # Add the file to the buffer (API Call)
        newFile.execute(file=file, dataset=dataset, conn = myThread.transaction.conn, transaction=myThread.transaction)
        myThread.transaction.commit()
        return	

    def addFJRFile(self, file, dataset=0):

        myThread = threading.currentThread()
        myThread.transaction.begin()

        wmbsFile = File(lfn = file['LFN'], size = file['Size'], events = file['TotalEvents'], 
				cksum=file['Checksum'])


	runLumiList=file.getLumiSections()
	runList=[x['RunNumber'] for x in runLumiList]
	for runNumber in runList:
		lumis = [int(y['LumiSectionNumber']) for y in runLumiList if y['RunNumber']==runNumber]
		run=Run(runNumber, *lumis)
		wmbsFile.addRun(run)

        if wmbsFile.exists() == False: 
		wmbsFile.create()
	else: 
		wmbsFile.load()
 	wmbsFile.setLocation(se=file['SEName'], immediateSave = True)
	# Lets add the file to DBS Buffer as well
	if dataset==0: 
		dataset=file.dataset[0]
	self.addFile(file, dataset)
	#Parent files
        for inputFile in file.inputFiles:
		#Parent file in this case doesn't get added to DBSBuffer
		parentFile = File(lfn=inputFile['LFN'])

		if parentFile.exists() == False: 
			parentFile.create() 
		else: 
			parentFile.load()
		try:
			wmbsFile.addParent(parentFile['lfn'])
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
        
        factory = WMFactory("dbsBuffer", "WMComponent.DBSBuffer.Database."+ \
                        myThread.dialect)
        newDS = factory.loadObject("NewDataset")
        newDS.execute(dataset=dataset, conn = myThread.transaction.conn, transaction=myThread.transaction)
        myThread.transaction.commit()
        return
    
    def addAlgo(self, dataset):
        # Add the algo to the buffer (API Call)
        # dataset object contains the algo information
        myThread = threading.currentThread()
        myThread.transaction.begin()
        
        factory = WMFactory("dbsBuffer", "WMComponent.DBSBuffer.Database."+ \
                        myThread.dialect)
        newDS = factory.loadObject("NewAlgo")
        newDS.execute(dataset=dataset, conn = myThread.transaction.conn, transaction=myThread.transaction)
        myThread.transaction.commit()
        return

    def updateAlgo(self, dataset, psethash):
        # Add the algo to the buffer (API Call)
        # dataset object contains the algo information
        myThread = threading.currentThread()
        myThread.transaction.begin()
        
        factory = WMFactory("dbsBuffer", "WMComponent.DBSBuffer.Database."+ \
                        myThread.dialect)
        newDS = factory.loadObject("UpdateAlgo")
        newDS.execute(dataset=dataset, psethash=psethash, conn = myThread.transaction.conn, transaction=myThread.transaction)
        myThread.transaction.commit()
        return
    
