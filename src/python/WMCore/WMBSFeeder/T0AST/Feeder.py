#!/usr/bin/env python
#pylint: disable-msg=W0613
"""
_Feeder_
"""
__revision__ = "$Id: Feeder.py,v 1.1 2009/11/06 11:06:24 riahi Exp $"
__version__ = "$Revision: 1.1 $"

import logging
import os
import threading

from WMCore.WMBSFeeder.FeederImpl import FeederImpl 
from WMCore.WMBS.File import File
from WMCore.DataStructs.Run import Run
from WMCore.WMInit import WMInit
from WMCore.DAOFactory import DAOFactory
#from WMCore.WMFactory import WMFactory

import time
from WMCore.Services.Requests import JSONRequests

#StartTime = int(time.time())
LastTime = int(time.time()) 
#lock = threading.Lock()

class Feeder(FeederImpl):
    """
    feeder implementation
    """

    def __init__(self, \
                 T0Url = "/tier0/listbulkfilesoverinterval/"): 
        """
        Configure the feeder
        """

        FeederImpl.__init__(self)

        self.myThread = threading.currentThread()
        self.maxRetries = 3
        self.purgeTime = 3600 #(86400 1 day)

        # Get configuration       
        self.init = WMInit()
        self.init.setLogging()
        self.init.setDatabaseConnection(os.getenv("DATABASE"), \
            os.getenv('DIALECT'), os.getenv("DBSOCK"))

        self.daofactory = DAOFactory(package = "WMCore.WMBS" , \
              logger = self.myThread.logger, \
              dbinterface = self.myThread.dbi)
        self.locationNew = self.daofactory(classname = "Locations.New")

        #factory = WMFactory("default", \
        #    "WMComponent.FeederManager.Database." + self.myThread.dialect)
        #self.queries = factory.loadObject("Queries")

    def __call__(self, filesetToProcess):
        """
        The algorithm itself
        """
        global LastTime    
        #global StartTime 

        logging.debug("the T0Feeder is processing %s" % \
                 filesetToProcess.name) 
        logging.debug("the fileset name %s" % \
         (filesetToProcess.name).split(":")[0])

        # url builder
        primaryDataset = ((filesetToProcess.name).split(":")[0]).split('/')[1]
        processedDataset = ((filesetToProcess.name).split(":")[0]).split('/')[2]
        dataTier = ((filesetToProcess.name\
            ).split(":")[0]).split('/')[3]
        url = "/tier0/listbulkfilesoverinterval/%s/%s/%s/%s" % \
              (LastTime, primaryDataset,processedDataset, dataTier)

        tries = 1
 
        while True:

            try:

                myRequester = JSONRequests(url = "vocms52.cern.ch:8080")
                requestResult = myRequester.get(url)
                newFilesList = requestResult[0]["results"] 
                logging.debug(newFilesList)

            except:
                logging.debug("T0Reader call error...")

                if tries == self.maxRetries:
                    return  
                else:
                    tries += 1 
                    continue

            logging.debug("T0 queries done ...")
            now = time.time()
            filesetToProcess.last_update = now
            LastTime = int(newFilesList['end_time']) + 1

            break

        # process all files
        if len(newFilesList['files']):  

            for files in newFilesList['files']:
     
                logging.debug("Files to add %s" %files)

                # Assume parents and LumiSection aren't asked 
                newfile = File(str(files['lfn']), \
           size = files['file_size'], events = files['events'])
                self.locationNew.execute(siteName = "caf.cern.ch")
                newfile.setLocation(["caf.cern.ch"])

                for run in files['runs']:
                    newfile.addRun(Run( run , *files['runs'][run]))

                newfile.create()
                filesetToProcess.addFile(newfile)
                logging.debug("new file added...")

        else:
            logging.debug("nothing to do...")

        # Commit the fileset
        filesetToProcess.commit()

        # Purge work - sleep for one day blocking all T0 process 
        # before doing the purge work 
        # To create all needed jobs with files acquired until now 
        # Remove T0 filesets from management will be the purge work 
        #lock.acquire()
        #if int(now) > (startTime + self.purgeTime):
        #    time.sleep(180) #(24 hours)
        #    logging.debug("Purging fileset...id %s name %s" \
        # %(filesetToProcess.id,filesetToProcess.name))
        #    dict = self.queries.getManagedFilesets("T0")
        #    for filesetId in dict:
        #       self.queries.removeManagedFilesets(filesetId, "T0")
        #       self.queries.closeFileset(filesetId)
        #    self.queries.purgeFilesets("T0")
            #for fileset in dict:
            #    newFileset = Fileset( name = dict[fileset] )
            #    newFileset.create()
            #    self.queries.addFilesetToManage(newFileset.id,"T0")
        #    startTime = int(time.time())
        #    logging.debug("Purge Done...")
        #lock.release()

    def persist(self):
        """
        To overwrite
        """ 
        pass



