"""
ArchiveDataPoller
"""

from __future__ import (division, print_function) 
import logging
import traceback
from Utils.IterTools import grouper
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.Services.WMArchiver.DataMap import createArchiverDoc
from WMCore.Services.WMArchiver.WMArchiver import WMArchiver
from WMCore.Services.FWJRDB.FWJRDBAPI import FWJRDBAPI

class ArchiveDataPoller(BaseWorkerThread):
    """
    Gather fwjr data and update to archiver\
    """
    
    def __init__(self, config):
        """
        initialize properties specified from config
        """
        BaseWorkerThread.__init__(self)
        self.config = config
                         
    def setup(self, parameters):
        """
        set db connection(couchdb, wmbs) to prepare to gather information
        """
        baseURL = self.config.JobStateMachine.couchurl
        dbname = "%s/fwjrs" % getattr(self.config.JobStateMachine, "couchDBName")
         
        self.fwjrAPI = FWJRDBAPI(baseURL, dbname)
        self.wmarchiver = WMArchiver(self.config.ArchiveDataReporter.WMArchiverURL)
        self.numDocsRetrievePerPolling = getattr(self.config.ArchiveDataReporter, "numDocsRetrievePerPolling", 1000)
        self.numDocsUploadPerCall = getattr(self.config.ArchiveDataReporter, "numDocsUploadPerCall", 200)
        
    def algorithm(self, parameters):
        """
        get information from wmbs, workqueue and local couch
        """
        try:
            logging.info("Getting not archived data info from FWRJ db...")
            data = self.fwjrAPI.getFWJRByArchiveStatus('ready', limit=1000)['rows']
            
            for slicedData in grouper(data, self.numDocsUploadPerCall):
                jobIDs = []
                archiverDocs = []
                for job in slicedData:
                    doc = createArchiverDoc(job)
                    archiverDocs.append(doc)
                    jobIDs.append(job["id"])
                    
                response = self.wmarchiver.archiveData(archiverDocs)
            
                # Partial success is not allowed either all the insert is successful of none is successful.
                if response[0]['status'] == "ok" and len(response[0]['ids']) == len(jobIDs):
                    for docID in jobIDs:
                        self.fwjrAPI.updateArchiveUploadedStatus(docID)
        except Exception as ex:
            logging.error("Error occurred, will retry later:")
            logging.error(str(ex))
            logging.error("Trace back: \n%s" % traceback.format_exc())
