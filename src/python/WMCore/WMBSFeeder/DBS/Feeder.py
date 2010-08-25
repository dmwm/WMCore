#!/usr/bin/env python
#pylint: disable-msg=W0613
"""
_Feeder_
"""
__all__ = []
__revision__ = "$Id: Feeder.py,v 1.2 2009/07/14 13:30:36 riahi Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "simon"

# DBS2
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
from DBSAPI.dbsApiException import DbsConnectionError
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSReaderError

import logging
import time

#from WMCore.WMBSFeeder.Registry import registerFeederImpl
from WMCore.WMBSFeeder.FeederImpl import FeederImpl 
from WMCore.WMBS.File import File

class Feeder(FeederImpl):
    """
    Feeder implementation
    """

    def __init__(self, purgeTime = 48,
                 phedexUrl = \
           "http://cmsweb.cern.ch/phedex/datasvc/json/prod/fileReplicas",
                 dbsUrl = \
         "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"): 
                 
        """
        Configure the feeder
        """

        FeederImpl.__init__(self)

        # External source parameter
        self.phedexUrl = phedexUrl

        # DBS parameter
        self.dbsReader = DBSReader(dbsUrl)

        #self.myThread = threading.currentThread()

        # FIXME: it has to be done by another object 
        # Get configuration       
        # self.wmConf = WMConf(backend = os.getenv('DIALECT'))
        # self.wmConf.setLogging()
        # self.wmConf.setDatabaseConnection()

        #self.daofactory = DAOFactory(package = "WMCore.WMBS" , \
        #      logger = self.myThread.logger, \
        #      dbinterface = self.myThread.dbi)
        #self.locationExist = self.daofactory(classname = "Locations.Exists")
        #self.locationNew = self.daofactory(classname = "Locations.New")

        # The last run that was identified as new, and run purge time
        self.purgeTime = purgeTime * 3600 # Convert hours to seconds

        # DBS parameter
        self.dbsReader = DBSReader(dbsUrl)
    
    def __call__(self, fileset):
        """
        The algorithm itself
        """
     
        logging.debug("the Feeder Feeder is processing %s" % \
                 fileset.name) 
    
        # get list of files
        tries = 1

        while True:
 
            try:

                now = time.time()  
                blocks = self.dbsReader.getFiles(fileset.name)
                fileset.last_update = now
                logging.debug("DBS queries done ...")

                break

            except DBSReaderError, ex:
                logging.error("DBS error: %s, cannot get files for %s" % \
                          (str(ex), fileset.name))
                return fileset 

            # connection error, retry
            except DbsConnectionError, ex:
                logging.error("Unable to connect to DBS, retrying: " + \
                          str(ex))
                if tries > self.connectionAttempts: #too many errors - bail out
                    return fileset
                tries = tries + 1

        # check for empty datasets
        if blocks == {}:
            logging.debug("DBS: Empty blocks - %s" %fileset.name)  
            return fileset 
    
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

            #else:

            #    for loc in seList:

            #        if not self.locationExist.execute(site_name = loc):
            #            self.locationNew.execute(sename = loc)
  
            
            for files in blocks[fileBlock]['Files']:
                
                # Assume parents and LumiSection aren't asked 
                newfile = File(files['LogicalFileName'], size=files['FileSize'],
                                 events=files['NumberOfEvents'])
                                 #lumi=files['LumiList'], locations=seList)

                newfile.create()
                newfile.setLocation(seList)
                fileset.addFile(newfile)

        # Commit the fileset
        fileset.commit()
        logging.debug("fileset %s is commited"% fileset.name)

    def persist(self):
        """
        To overwrite
        """ 
        pass

#registerFeederImpl(Feeder.__name__, Feeder)


