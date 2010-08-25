#!/usr/bin/env python
#pylint: disable-msg=W0613
"""
_Feeder_
"""
__revision__ = "$Id: Feeder.py,v 1.9 2009/12/15 23:07:28 riahi Exp $"
__version__ = "$Revision: 1.9 $"

from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.Services.DBS.DBSErrors import DBSReaderError

import logging
import time
import os
import threading

from WMCore.WMBSFeeder.FeederImpl import FeederImpl 
from WMCore.WMBS.File import File
from WMCore.DAOFactory import DAOFactory
from WMCore.WMInit import WMInit

from DBSAPI.dbsApiException import DbsConnectionError 
from DBSAPI.dbsApi import DbsApi

#from DBSAPI.dbsException import *
#from DBSAPI.dbsApiException import *

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

        self.dbsReader = DBSReader(dbsUrl)
        self.connectionAttempts = 5 
        self.myThread = threading.currentThread()

        # Get configuration       
        self.init = WMInit()
        self.init.setLogging()
        self.init.setDatabaseConnection(os.getenv("DATABASE"), \
            os.getenv('DIALECT'), os.getenv("DBSOCK"))

        self.daofactory = DAOFactory(package = "WMCore.WMBS" , \
              logger = self.myThread.logger, \
              dbinterface = self.myThread.dbi)
        self.locationNew = self.daofactory(classname = "Locations.New")
        self.getFileLoc = self.daofactory(classname = "Files.GetLocation")
    
    def __call__(self, filesetToProcess):
        """
        The algorithm itself
        """
 
        logging.debug("DBSFeeder is processing %s" % \
                 filesetToProcess.name) 
        logging.debug("the fileset name  %s" \
       % (filesetToProcess.name).split(":")[0])
  
        # get list of files
        tries = 1
 
        while True:
 
            try:

                blocks = self.dbsReader.getFiles(\
              (filesetToProcess.name).split(":")[0])

                now = time.time()  
                filesetToProcess.last_update = now
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

        # Get the start Run if asked
        startRun = (filesetToProcess.name).split(":")[3]    

        # process all file blocks
        for fileBlock in blockList:

            try:
  
                files = self.dbs.listFiles("", "", "", [], "", fileBlock, \
                details = None,retriveList = ['retrive_run' ])

            #except DbsException, ex:
            except:
 
                msg = "Error in "
                msg += "listFilesRun(%s)\n" % (
                    fileBlock, )
                raise DBSReaderError(msg)

            if int(startRun) > int(\
          files[0]['RunsList'][0]['RunNumber']) :                  
                continue

            # get fileBlockId SE information
            seList = blocks[fileBlock]['StorageElements']
 
            # add files for non blocked SE
            if seList is None or seList == []:
                logging.info("fileblock %s - no SE's associated" % \
                        fileBlock)
                continue

            else:
 
                for loc in seList:
                    self.locationNew.execute(siteName = loc)
 
            for files in blocks[fileBlock]['Files']:

                # Assume parents and LumiSection aren't asked 
                newfile = File(files['LogicalFileName'], size=files['FileSize'],
                                 events=files['NumberOfEvents'])
                               #lumi=files['LumiList']), locations=seList)
 
                if newfile.exists() == False :
                    newfile.create()

                fileLoc = self.getFileLoc.execute(\
                file = files['LogicalFileName'])

                if fileLoc:

                    for loc in seList:

                        if loc not in fileLoc: 
  
                            newfile.setLocation(loc)

                        else:

                            logging.debug("File already associated to %s" %loc)

                else:

                    newfile.setLocation(seList) 

                filesetToProcess.addFile(newfile)

        # Close fileset
        filesetToProcess.markOpen(False)

        # Commit the fileset
        filesetToProcess.commit()

        logging.debug("fileset %s is commited"% filesetToProcess.name)

    def persist(self):
        """
        To overwrite
        """ 
        pass



