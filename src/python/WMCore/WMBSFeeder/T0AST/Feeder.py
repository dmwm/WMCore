#!/usr/bin/env python
#pylint: disable-msg=W0613,E1103
"""
_Feeder_
"""



import logging
import threading

from WMCore.WMBSFeeder.FeederImpl import FeederImpl 
from WMCore.WMBS.File import File
#from WMCore.WMBS.Fileset import Fileset
from WMCore.DataStructs.Run import Run
from WMCore.DAOFactory import DAOFactory
#from WMCore.WMFactory import WMFactory
from traceback import format_exc
import os
from WMCore.WMInit import WMInit

import time
from WMCore.Services.Requests import JSONRequests

LOCK = threading.Lock()

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
        self.purgeTime = 96 
        self.reopenTime = 120 

    def __call__(self, filesetToProcess):
        """
        The algorithm itself
        """
        global LOCK

        # Get configuration
        initObj = WMInit()
        initObj.setLogging()
        initObj.setDatabaseConnection(os.getenv("DATABASE"), \
            os.getenv('DIALECT'), os.getenv("DBSOCK"))

        myThread = threading.currentThread()

        daofactory = DAOFactory(package = "WMCore.WMBS" , \
              logger = myThread.logger, \
              dbinterface = myThread.dbi)

        locationNew = daofactory(classname = "Locations.New")
        getFileLoc = daofactory(classname = "Files.GetLocation")
        fileInFileset = daofactory(classname = "Files.InFileset")


        logging.debug("the T0Feeder is processing %s" % \
                 filesetToProcess.name) 
        logging.debug("the fileset name %s" % \
         (filesetToProcess.name).split(":")[0])

        # Get the start Run if asked
        startRun = (filesetToProcess.name).split(":")[3]
        fileType = (filesetToProcess.name).split(":")[2]

        
        LASTIME = filesetToProcess.lastUpdate
 
        # url builder
        primaryDataset = ((filesetToProcess.name).split(":")[0]).split('/')[1]
        processedDataset = ((filesetToProcess.name).split(":")[0]).split('/')[2]
        dataTier = ((filesetToProcess.name\
            ).split(":")[0]).split('/')[3]
        url = "/tier0/listfilesoverinterval/%s/%s/%s/%s/%s" % \
              (fileType, LASTIME, primaryDataset,processedDataset, dataTier)

        tries = 1
        while True:

            try:

                myRequester = JSONRequests(url = "vocms52.cern.ch:8889")
                requestResult = myRequester.get(\
              url+"/"+"?return_type=text/json%2Bdas")
                newFilesList = requestResult[0]["results"]

            except:

                logging.debug("T0Reader call error...")
                if tries == self.maxRetries:
                    return  
                else:
                    tries += 1 
                    continue

            logging.debug("T0 queries done ...")
            now = time.time()
            LASTIME = int(newFilesList['end_time']) + 1

            break

        # process all files
        if len(newFilesList['files']):  
     
            try:
                locationNew.execute(siteName = "caf.cern.ch", seName = "caf.cern.ch")
            except Exception,e:
                logging.debug("Error when adding new location...")
                logging.debug(e)
                logging.debug( format_exc() )

            for files in newFilesList['files']:

                # Assume parents aren't asked 
                newfile = File(str(files['lfn']), \
           size = files['file_size'], events = files['events'])


                try:

                    LOCK.acquire() 

                    if newfile.exists() == False :
                        newfile.create()

                        for run in files['runs']:

                            runSet = set()
                            runSet.add(Run( run, *files['runs'][run]))
                            newfile.addRunSet(runSet)

                    else:
                        newfile.loadData()

                    fileLoc = getFileLoc.execute(file = files['lfn'])

                    if 'caf.cern.ch' not in fileLoc:
                        newfile.setLocation("caf.cern.ch")

#                    else:
#                        logging.debug("File already associated to %s" %fileLoc)


                    LOCK.release()

                    if len(newfile["runs"]):

                        val = 0
                        for run in newfile['runs']:

                            if run.run < int(startRun):
                                val = 1 
                                break 

                        if not val:

                            listFile = fileInFileset.execute\
                               (filesetToProcess.id)
                            if {'fileid': newfile['id']} not in listFile:

                                filesetToProcess.addFile(newfile)
                                filesetToProcess.setLastUpdate\
                              (int(newFilesList['end_time']) + 1)
                                filesetToProcess.commit()
                                logging.debug("new file created/loaded added by T0AST...")


                except Exception,e:
                    logging.debug("Error when adding new files in T0AST...")
                    logging.debug(e)
                    logging.debug( format_exc() )
                    LOCK.release()

            filesetToProcess.commit()

        else:

            logging.debug("nothing to do in T0AST...")
            # For reopned fileset or empty 
            # try until the purge time is reached            
            if (int(now)/3600 - LASTIME/3600) > self.reopenTime:

                filesetToProcess.setLastUpdate(time.time())
                filesetToProcess.commit()


        # Commit the fileset
        logging.debug("Test purge in T0AST ...")
        filesetToProcess.load()
        LASTIME = filesetToProcess.lastUpdate

        if (int(now)/3600 - LASTIME/3600) > self.purgeTime:

            filesetToProcess.markOpen(False)
            logging.debug("Purge Done...")

        filesetToProcess.commit()

    def persist(self):
        """
        To overwrite
        """ 
        pass



