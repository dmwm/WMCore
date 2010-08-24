#!/usr/bin/env python
#pylint: disable-msg=W0613,E1103
"""
_Feeder_
"""



import logging
import threading

from WMCore.WMBSFeeder.FeederImpl import FeederImpl 
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.DataStructs.Run import Run
from WMCore.DAOFactory import DAOFactory
#from WMCore.WMFactory import WMFactory
from traceback import format_exc
from WMCore.WMInit import WMInit
import os

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


        logging.debug("the T0Feeder is processing %s" % \
                 filesetToProcess.name) 
        logging.debug("the fileset name %s" % \
         (filesetToProcess.name).split(":")[0])

        startRun = (filesetToProcess.name).split(":")[3]
        fileType = (filesetToProcess.name).split(":")[2]

        # url builder
        primaryDataset = ((filesetToProcess.name).split(":")[0]).split('/')[1]
        processedDataset = ((filesetToProcess.name).split(":")[0]).split('/')[2]
        dataTier = (((filesetToProcess.name\
            ).split(":")[0]).split('/')[3]).split('-')[0]

        # Fisrt call to T0 db for this fileset 
        # Here add test for the closed fileset 
        LASTIME = filesetToProcess.lastUpdate

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

            logging.debug("T0ASTRun queries done ...")
            now = time.time()
            filesetToProcess.last_update = now
            LASTIME = int(newFilesList['end_time']) + 1

            break



        # process all files
        if len(newFilesList['files']):  

            LOCK.acquire()

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
                    if newfile.exists() == False :
                        newfile.create()

                    else:
                        newfile.loadData()

                    #Add run test if already exist
                    for run in files['runs']:

                        if startRun != 'None' and int(startRun) <= int(run):

                            # ToDo: Distinguish between 
                            # filestA-RunX and filesetA-Run[0-9]*
                            filesetRun = Fileset( name = (((\
                   filesetToProcess.name).split(':')[0]).split('/')[0]\
                   )+'/'+(((filesetToProcess.name).split(':')[0]).split\
                   ('/')[1])+'/'+(((filesetToProcess.name).split(':')[0]\
                   ).split('/')[2])+'/'+((((filesetToProcess.name).split\
                   (':')[0]).split('/')[3]).split('-')[0])+'-'+'Run'+str\
               (run)+":"+":".join((filesetToProcess.name).split(':')[1:] \
                                     ) )


                            if filesetRun.exists() == False :
                                filesetRun.create()

                            else:
                                filesetRun.loadData()
   
                            # Add test runs already there 
                            # (for growing dataset) - 
                            # to support file with different runs and lumi
                            if not newfile['runs']:

                                runSet = set()
                                runSet.add(Run( run, *files['runs'][run]))
                                newfile.addRunSet(runSet)

                            fileLoc = getFileLoc.execute(file = files['lfn'])

                            if 'caf.cern.ch' not in fileLoc:
                                newfile.setLocation("caf.cern.ch")


                            filesetRun.addFile(newfile)
                            logging.debug("new file created/loaded added by T0ASTRun...")
                            filesetRun.commit()

                except Exception,e:

                    logging.debug("Error when adding new files in T0ASTRun...")
                    logging.debug(e)
                    logging.debug( format_exc() )



                filesetToProcess.setLastUpdate\
              (int(newFilesList['end_time']) + 1)
                filesetToProcess.commit()

            LOCK.release()

        else:

            logging.debug("nothing to do...")
            # For re-opned fileset or empty, try until the purge time
            if (int(now)/3600 - LASTIME/3600) > self.reopenTime:

                filesetToProcess.setLastUpdate(time.time())
                filesetToProcess.commit()


        if LASTIME: 

            myRequester = JSONRequests(url = "vocms52.cern.ch:8889")
            requestResult = myRequester.get("/tier0/runs")

            for listRun in requestResult[0]:

                if int(startRun) <= int(listRun['run']):
 
                    if listRun['status'] =='CloseOutExport' or \
           listRun['status'] =='Complete' or listRun['status'] ==\
                          'CloseOutT1Skimming':        
 
                        closeFileset = Fileset( name = (((\
      filesetToProcess.name).split(':')[0]).split('/')[0])+'/'+\
     (((filesetToProcess.name).split(':')[0]).split('/')[1]\
     )+'/'+(((filesetToProcess.name).split(':')[0]).split('/')\
     [2])+'/'+((((filesetToProcess.name).split(':')[0]).split\
     ('/')[3]).split('-')[0])+'-'+'Run'+str(listRun['run'])\
     +":"+":".join((filesetToProcess.name).split(':')[1:] ) )
     
                        if closeFileset.exists() != False :

                            closeFileset = Fileset( id = closeFileset.exists())
                            closeFileset.loadData()

                            if closeFileset.open == True:      
                                closeFileset.markOpen(False)


        # Commit the fileset
        filesetToProcess.commit()


        # Commit the fileset
        logging.debug("Test purge in T0ASTRun ...")
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



