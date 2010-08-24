#!/usr/bin/env python

from WMCore.WMBS.WMBSFeeder.Registry import registerFeederImpl
from WMCore.WMBS.WMBSFeeder.FeederImpl import FeederImpl 
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset

import logging
import time

# DBS2
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
from ProdCommon.DataMgmt.DBS.DBSWriter import DBSWriter
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSReaderError, DBSWriterError
from DBSAPI.dbsApiException import DbsConnectionError


class DBSFeeder(FeederImpl):
    """
    DBS feeder plugin
    
    Get files for a fileset
    
    """
#            """
#        _getFileListFromDBS_
#        
#        Get the list of files of the dataset from DBS
#        
#        Arguments:
#            
#          datasetId -- the name of the dataset
#          
#        Return:
#            
#          dictionary with index 'file block' and ech value defined as 
#          a dictionary with index 'file name' and each value defined
#          as a dictionary with at least 'NumberOfEvents', 'FileSize'.
#          
#        """

    def __init__(self):
        FeederImpl.__init__(self)
        self.dbsReader = None


    def __call__(self, fileset):
        # list of files
        fileList = []
    
        # get list of files
        tries = 1
        while True:
 
            try:
                self.dbsReader = DBSReader(fileset.sourceURL)
                now = time.time()   # DBS doesnt provide the last update time
                blocks = self.dbsReader.getFiles(fileset.name)
                fileset.last_update = now
                break

            except DBSReaderError, ex:
                logging.error("DBS error: %s, cannot get files for %s" % \
                          (str(ex), fileset.name))
                return fileList

            # connection error, retry
            except DbsConnectionError, ex:
                logging.error("Unable to connect to DBS, retrying: " + \
                          str(ex))
                if tries > self.connectionAttempts: #too many errors - bail out
                    return fileList
                tries = tries + 1

        # check for empty datasets
        if blocks == {}:
            return fileset #fileList
    
        # get all file blocks
        blockList = blocks.keys()
    
        # process all file blocks
        for fileBlock in blockList:
    
            # get fileBlockId SE information
            seList = blocks[fileBlock]['StorageElements']
            
            # add files for non blocked SE
            #if seList is None or seList == []:
            #    #logging.info("fileblock %s blocked - no SE's associated" % fileBlock)
            #    continue
            
            for file in blocks[fileBlock]['Files']:
                
#            Assume parents will always be in wmbs before children
#                Uncomment to wait until they arrive
#                parents = [ File(x) for x in file['ParentList']]
#                for parent in parents:
#                    if not parent.exists():
#                        # ignore this file till parent imported
#                        continue
                
                newfile = File(file['LogicalFileName'], size=file['FileSize'],
                                 events=file['NumberOfEvents'],
                                 run=file['RunsList'], lumi=file['LumiList'],
                                 parents=file['ParentList'], locations=seList,
                                 wmbs=fileset.wmbs)
                
                # check file doesn't exist in wmbs
                if file in fileset.listFiles():
                    continue
                
                #fileList.append(newfile)
                fileset.addFile(newfile)

        #return fileList
        return fileset

    #TODO: See about modifying to a migrate function that would work to global
    def importFileset(self, fileset, source, dest, parentageLevel=0): #migrate
        """
        Migrate given fileset (plus optional parents) from source DBS to local
        """
        
        #TODO: Check we really want to always import parentage
        tries = 1
        while True:
            try:
                dbsWriter = DBSWriter(dest)
                dbsWriter.importDataset(source, fileset.name, dest, True)
                if fileset.source =='dbs':
                    fileset.sourceURL = dest # now get files from local dbs
                break

            except DBSWriterError, ex:
                logging.error("DBS error: %s, cannot import %s" % \
                          (str(ex), fileset.name))
                return

            # connection error, retry
            except DbsConnectionError, ex:
                logging.error("Unable to connect to DBS, retrying: " + \
                          str(ex))
                if tries > self.connectionAttempts: #too many errors - bail out
                    raise
                tries = tries + 1
            
        return
        
        
    def fillFilesetParentage(self, fileset, dbs, parentageLevel=0):
        """
        Populate the fileset with parents to the desired level
            Assume parents are complete and no new files will be added
        """
        current = fileset
        while parentageLevel:
            tries = 1
            while True:
                try:
                    dbsReader = DBSReader(dbs)
                    parents = dbsReader.listParentage(current.name)
                    current.parents = [Fileset(name, current.wmbs,
                                        is_open=False, source=current.source,
                                        sourceURL=current.sourceURL).create() for name in parents]
                    parentageLevel = parentageLevel - 1
                    break
                except DBSReaderError, ex:
                    logging.error("DBS error: %s, cannot get parents for %s" % \
                          (str(ex), current.name))
                    raise

                # connection error, retry
                except DbsConnectionError, ex:
                    logging.error("Unable to connect to DBS, retrying: " + \
                              str(ex))
                    if tries > self.connectionAttempts: #too many errors - bail out
                        raise
                    tries = tries + 1
        
        return fileset            
        
                
    def getParentsForNewFiles(self, fileset):
        """
        Get parentage for files - used by other feeders 
        that do not contain parentage info
        """
        #TODO: make recursive for parentageLevel > 1
        # first check we need parents
        if not fileset.listNewFiles() or not fileset.parents or \
                                    fileset.listNewFiles()[0].parents():
            return fileset
        
        for file in fileset.listNewFiles():
            tries = 1
            while True:
                try:
                    dbsReader = DBSReader(fileset.source)
                    parents = dbsReader.getFileParentsLFN(file['LFN'])
                    file.parents = set(parents) #file can handle string of LFNS - if already in db
                    break
                except DBSReaderError, ex:
                        logging.error("DBS error: %s, cannot get parents for %s" % \
                              (str(ex), file['LFN']))
                        raise
                # connection error, retry
                except DbsConnectionError, ex:
                    logging.error("Unable to connect to DBS, retrying: " + \
                              str(ex))
                    if tries > self.connectionAttempts: #too many errors - bail out
                        raise
                    tries = tries + 1
                    
        return fileset


registerFeederImpl(DBSFeeder.__name__, DBSFeeder)