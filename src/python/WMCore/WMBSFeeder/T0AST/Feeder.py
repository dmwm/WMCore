#!/usr/bin/env python
#pylint: disable-msg=W0613,E1103
"""
_Feeder_
"""
__revision__ = "$Id: Feeder.py,v 1.4 2009/12/28 04:41:54 riahi Exp $"
__version__ = "$Revision: 1.4 $"

import logging
import threading

from WMCore.WMBSFeeder.FeederImpl import FeederImpl 
from WMCore.WMBS.File import File
#from WMCore.WMBS.Fileset import Fileset
from WMCore.DataStructs.Run import Run
from WMCore.DAOFactory import DAOFactory
#from WMCore.WMFactory import WMFactory
from traceback import format_exc

import time
from WMCore.Services.Requests import JSONRequests

#StartTime = int(time.time())
LASTIME = int(time.time()) 

class Feeder(FeederImpl):
    """
    feeder implementation
    """

    def __init__(self, \
                 urlT0 = "/tier0/listbulkfilesoverinterval/"): 
        """
        Configure the feeder
        """

        FeederImpl.__init__(self)

        self.maxRetries = 3
        self.purgeTime = 3600 #(86400 1 day)

        myThread = threading.currentThread()
        self.daofactory = DAOFactory(package = "WMCore.WMBS" , \
              logger = myThread.logger, \
              dbinterface = myThread.dbi)

        #factory = WMFactory("default", \
        #    "WMComponent.FeederManager.Database." + self.myThread.dialect)
        #self.queries = factory.loadObject("Queries")

    def __call__(self, filesetToProcess):
        """
        The algorithm itself
        """
        global LASTIME    

        #global StartTime 
        #filesetToProcess = Fileset( name = filesetToProcess.name )
        #filesetToProcess.load() 

        myThread = threading.currentThread()

        locationNew = self.daofactory(classname = "Locations.New")
        getFileLoc = self.daofactory(classname = "Files.GetLocation")

        logging.debug("the T0Feeder is processing %s" % \
                 filesetToProcess.name) 
        logging.debug("the fileset name %s" % \
         (filesetToProcess.name).split(":")[0])

        fileType = (filesetToProcess.name).split(":")[2]
        logging.debug("the fileType is %s" % \
        (filesetToProcess.name).split(":")[2])

        # url builder
        primaryDataset = ((filesetToProcess.name).split(":")[0]).split('/')[1]
        processedDataset = ((filesetToProcess.name).split(":")[0]).split('/')[2]
        dataTier = ((filesetToProcess.name\
            ).split(":")[0]).split('/')[3]
        url = "/tier0/listfilesoverinterval/%s/%s/%s/%s/%s" % \
              (fileType, LASTIME, primaryDataset,processedDataset, dataTier)
        #url = "/tier0/listbulkfilesoverinterval/%s/%s/%s/%s" % \
        #      (fileType, filesetToProcess.lastUpdate, primaryDataset,\
        #               processedDataset, dataTier)

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
            LASTIME = int(newFilesList['end_time']) + 1

            break

        # process all files
        if len(newFilesList['files']):  

     
            try:
                locationNew.execute(siteName = "caf.cern.ch")
            except Exception,e:
                logging.debug("Error when adding new location...")
                logging.debug(e)
                logging.debug( format_exc() )

            for files in newFilesList['files']:
                logging.debug("Files to add %s" %files)
                # Assume parents and LumiSection aren't asked 
                newfile = File(str(files['lfn']), \
           size = files['file_size'], events = files['events'])


                for run in files['runs']:
                    newfile.addRun(Run( run , *files['runs'][run]))

                try:
                    if newfile.exists() == False :
                        newfile.create()

                    fileLoc = getFileLoc.execute(file = files['lfn'])

                    if 'caf.cern.ch' not in fileLoc:
                        newfile.setLocation(["caf.cern.ch"])

                    else:
                        logging.debug("File already associated to %s" %fileLoc)

                    filesetToProcess.addFile(newfile)
                    logging.debug("new file added...")
                except Exception,e:
                    logging.debug("Error when adding new location...")
                    logging.debug(e)
                    logging.debug( format_exc() )


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



