#!/usr/bin/env python
#pylint: disable-msg=W0613
"""
_Feeder_
"""
__revision__ = "$Id: Feeder.py,v 1.8 2009/11/06 10:52:24 riahi Exp $"
__version__ = "$Revision: 1.8 $"

# DBS
from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.Services.DBS.DBSErrors import DBSReaderError
from DBSAPI.dbsApiException import DbsConnectionError 

import logging
import time
import os
import threading

from WMCore.WMBSFeeder.FeederImpl import FeederImpl 
from WMCore.WMBS.File import File
from WMCore.DAOFactory import DAOFactory
from WMCore.WMInit import WMInit

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
    
        # process all file blocks
        for fileBlock in blockList:
    
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

                newfile.create()
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



