#!/usr/bin/env python
#pylint: disable-msg=W0613
"""
_Feeder_
"""
__revision__ = "$Id: Feeder.py,v 1.14 2010/05/04 22:53:07 riahi Exp $"
__version__ = "$Revision: 1.14 $"

from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.Services.DBS.DBSErrors import DBSReaderError

import logging
import time
import threading

from WMCore.WMBSFeeder.FeederImpl import FeederImpl 
from WMCore.WMBS.File import File
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.Fileset import Fileset

from DBSAPI.dbsApiException import DbsConnectionError 
from DBSAPI.dbsApi import DbsApi
from WMCore.DataStructs.Run import Run

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

        self.daofactory = DAOFactory(package = "WMCore.WMBS" , \
              logger = self.myThread.logger, \
              dbinterface = self.myThread.dbi)


    
    def __call__(self, filesetToProcess):
        """
        The algorithm itself
        """

 
        locationNew = self.daofactory(classname = "Locations.New")
        getFileLoc = self.daofactory(classname = "Files.GetLocation")

        logging.debug("DBSFeeder is processing %s" % \
                 filesetToProcess.name) 
        logging.debug("the filesetBase name  %s" \
       % (filesetToProcess.name).split(":")[0])
  





         ##########DBS Polling for filesetBase###############

        # Get only filesetBase
        if len((filesetToProcess.name).split(":")) < 3 :

            
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
                    #too many errors - bail out
                    if tries > self.connectionAttempts: 
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
                        locationNew.execute(siteName = loc)

                for files in blocks[fileBlock]['Files']:

                    if len(files['LumiList']):

                        for lumi in files['LumiList']:
    
                            newfile = File(files['LogicalFileName'], \
                            size=files['FileSize'], events=files\
                            ['NumberOfEvents'])

                            if newfile.exists() == False :
                                newfile.create()
                            else:
                                newfile.loadData()

                             #Add test runs already there 
                             #(for growing dataset to update 
                             #the lumi information)
                            if not newfile['runs']:

                                runSet = set()
                                runSet.add(Run( lumi['RunNumber' ], \
                                *[lumi['LumiSectionNumber']] )) 
                                newfile.addRunSet(runSet)

                            else:

                                val = 0
                                for run in newfile['runs']:
                                    if lumi['RunNumber' ] == run.run:
                                        val = 1

                                if not val:

                                    newfile.addRun(Run( \
                     lumi['RunNumber' ], *[lumi['LumiSectionNumber']]))
   
                            fileLoc = getFileLoc.execute(\
                                file = files['LogicalFileName'])
    
                            if fileLoc:
                                for loc in seList:
                                    if loc not in fileLoc:
                                        newfile.setLocation(loc)
                            else:
                                newfile.setLocation(seList)
                            filesetToProcess.addFile(newfile)

                    else:

                        # Assume parents and LumiSection aren't asked 
                        newfile = File(files['LogicalFileName'], \
                      size=files['FileSize'], events=files['NumberOfEvents'])
                        if newfile.exists() == False :
                            newfile.create()

                        fileLoc = getFileLoc.execute(\
                            file = files['LogicalFileName'])

                        if fileLoc:
                            for loc in seList:
                                if loc not in fileLoc:
                                    newfile.setLocation(loc)
                        else:
                            newfile.setLocation(seList)
                        filesetToProcess.addFile(newfile)

            # Commit the fileset
            filesetToProcess.commit()


        #########Local DB Polling#######################
        else:

            filesetBase = Fileset( name = (filesetToProcess.name).split\
            (":")[0] + ":" + (filesetToProcess.name).split(":")[1])
            filesetBase.loadData()
 
            # Get the start Run if asked
            startRun = (filesetToProcess.name).split(":")[3]

            for fileToAdd in filesetBase.files:

                if fileToAdd not in filesetToProcess.getFiles(type = "list"):

                    for currentRun in fileToAdd['runs']:

                        if currentRun.run >= int(startRun):

                            filesetToProcess.addFile(fileToAdd)
                            logging.debug("new file added...")

            # Commit the fileset
            filesetToProcess.commit()


    def persist(self):
        """
        To overwrite
        """ 
        pass



