#!/usr/bin/env python
#pylint: disable-msg=W0613
"""
_Feeder_
"""



from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.Services.DBS.DBSErrors import DBSReaderError

import logging
import time
import threading
import os

from WMCore.WMBSFeeder.FeederImpl import FeederImpl 
from WMCore.WMBS.File import File
from WMCore.DAOFactory import DAOFactory
from WMCore.WMInit import WMInit

from DBSAPI.dbsApiException import DbsConnectionError 
from DBSAPI.dbsApi import DbsApi
from WMCore.DataStructs.Run import Run

LOCK = threading.Lock()

class Feeder(FeederImpl):
    """
    feeder implementation
    """

    def __init__(self, dbsUrl = \
         "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"):
        """
        Configure the feeder
        """

        FeederImpl.__init__(self)

        # DBS parameter
        self.args = { "url" : dbsUrl, "level" : 'ERROR'}
        self.dbs = DbsApi(self.args)
        self.purgeTime = 96 
        self.reopenTime = 120

        self.dbsReader = DBSReader(dbsUrl)
        self.connectionAttempts = 5 

    
    def __call__(self, filesetToProcess):
        """
        The algorithm itself
        """

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


        logging.debug("DBSFeeder is processing %s" % \
                 filesetToProcess.name) 
        logging.debug("the filesetBase name  %s" \
       % (filesetToProcess.name).split(":")[0])

        LASTIME = filesetToProcess.lastUpdate

        # Get the start Run if asked
        startRun = (filesetToProcess.name).split(":")[3]

        # get list of files
        tries = 1

        while True:
 
            try:

                blocks = self.dbsReader.getFiles(\
              (filesetToProcess.name).split(":")[0])
                now = time.time()  
                logging.debug("DBS queries done ...")

                break

            except DBSReaderError, ex:
                logging.error("DBS error: %s, cannot get files for %s" % \
                      (str(ex), filesetToProcess.name))
                # Close fileset
                filesetToProcess.markOpen(False)
                return 

            # connection error, retry
            except DbsConnectionError, ex:
                logging.error("Unable to connect to DBS, retrying: " + \
                      str(ex))
                if tries > self.connectionAttempts: #too many errors - bail out
                    return  
                tries = tries + 1

        # check for empty datasets
        if blocks == {}:
            logging.debug("DBS: Empty blocks - %s" %filesetToProcess.name)  
            return filesetToProcess 
  
        # get all file blocks
        blockList = blocks.keys()

        # process all file blocks
        for fileBlock in blockList:

            seList = blocks[fileBlock]['StorageElements']

            # add files for non blocked SE
            if seList is None or seList == []:
                logging.info("fileblock %s - no SE's associated" % \
                        fileBlock)
                continue

            else:

                for loc in seList:
                    locationNew.execute(siteName = loc, seName = loc)

            for files in blocks[fileBlock]['Files']:
 
                if startRun != 'None':

                    if len(files['LumiList']):

                        for lumi in files['LumiList']:

                            if int(startRun) <= int(lumi['RunNumber' ]):

                              
                                newfile = File(files['LogicalFileName'], \
                                size=files['FileSize'], events=files\
                                ['NumberOfEvents'])

                                LOCK.acquire()

                                if newfile.exists() == False :

                                    newfile.create()
                                    filesetToProcess.addFile(newfile)
                                    filesetToProcess.setLastUpdate(\
                                         int(time.time()))
                                    filesetToProcess.commit()

                                    runSet = set()
                                    runSet.add(Run( lumi\
                    ['RunNumber' ], *[lumi['LumiSectionNumber']] ))
                                    newfile.addRunSet(runSet)

                                else:

                                    newfile.loadData()

                                    listFile = fileInFileset.execute\
                                           (filesetToProcess.id)
 
                                    if {'fileid': newfile[\
                                      'id']} not in listFile:

                                        filesetToProcess.addFile(newfile)
                                        filesetToProcess.setLastUpdate\
                                           (int(time.time()))
                                        filesetToProcess.commit()

                                    val = 0
                                    for run in newfile['runs']:
                                        if lumi['RunNumber' ] == run.run:
                                            val = 1
                                            break
   
                                    if not val:

                                        runSet = set()
                                        runSet.add(Run(\
                     lumi['RunNumber' ], *[lumi['LumiSectionNumber']]))
                                        newfile.addRunSet(runSet)
   
                                fileLoc = getFileLoc.execute(\
                                    file = files['LogicalFileName'])
    
                                if fileLoc:
                                    for loc in seList:
                                        if loc not in fileLoc:
                                            newfile.setLocation(\
                                                loc)
                                else:

                                    newfile.setLocation(seList)
                                LOCK.release()

                else:

                    # Assume parents and LumiSection aren't asked 
                    newfile = File(files['LogicalFileName'], \
                  size=files['FileSize'], events=files['NumberOfEvents'])

                    LOCK.acquire()            
                    if newfile.exists() == False :
                        newfile.create()

                        # Update fileset last update parameter
                        filesetToProcess.addFile(newfile)
                        logging.debug("new file created and added by DBS")
                        filesetToProcess.setLastUpdate(int(time.time()))
                        filesetToProcess.commit()

                    else:

                        newfile.loadData()

                        listFile = fileInFileset.execute(filesetToProcess.id)

                        if {'fileid': newfile['id']} not in listFile:

                            filesetToProcess.addFile(newfile)
                            logging.debug("new file loaded and added by DBS") 
                            filesetToProcess.setLastUpdate(int(time.time()))
                            filesetToProcess.commit()

                    fileLoc = getFileLoc.execute(\
                        file = files['LogicalFileName'])

                    if fileLoc:
                        for loc in seList:
                            if loc not in fileLoc:
                                newfile.setLocation(loc)
                    else:
                        newfile.setLocation(seList)
                    LOCK.release()


        filesetToProcess.load()
        LASTIME = filesetToProcess.lastUpdate

        # For re-opned fileset or empty, try until the purge time
        if (int(now)/3600 - LASTIME/3600) > self.reopenTime:

            filesetToProcess.setLastUpdate(int(time.time()))
            filesetToProcess.commit()

        if (int(now)/3600 - LASTIME/3600) > self.purgeTime:

            filesetToProcess.markOpen(False)
            logging.debug("Purge Done...")

        filesetToProcess.commit()

        logging.debug("DBS feeder work done...")


    def persist(self):
        """
        To overwrite
        """ 
        pass



