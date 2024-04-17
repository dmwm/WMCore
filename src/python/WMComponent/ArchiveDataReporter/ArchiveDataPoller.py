"""
ArchiveDataPoller
"""

from __future__ import (division, print_function)
import logging
import traceback
from Utils.IteratorTools import grouper
from Utils.Timers import timeFunction
from Utils.Utilities import getSize
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.Services.WMArchive.DataMap import createArchiverDoc
from WMCore.Services.WMArchive.WMArchive import WMArchive
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
        # setup size threshold to fit CMSWEB nginx/frontend, i.e. 8MB
        self.sizeThreshold = getattr(config.ArchiveDataReporter, "sizeThreshold", 8*1024*1024)

    def setup(self, parameters):
        """
        set db connection(couchdb, wmbs) to prepare to gather information
        """
        baseURL = self.config.JobStateMachine.couchurl
        dbname = "%s/fwjrs" % getattr(self.config.JobStateMachine, "couchDBName")

        self.fwjrAPI = FWJRDBAPI(baseURL, dbname)
        self.wmarchiver = WMArchive(self.config.ArchiveDataReporter.WMArchiveURL)
        self.numDocsRetrievePerPolling = getattr(self.config.ArchiveDataReporter, "numDocsRetrievePerPolling", 1000)
        self.numDocsUploadPerCall = getattr(self.config.ArchiveDataReporter, "numDocsUploadPerCall", 200)

    @timeFunction
    def algorithm(self, parameters):
        """
        get information from wmbs, workqueue and local couch
        """
        try:
            data = self.fwjrAPI.getFWJRByArchiveStatus('ready', limit=self.numDocsRetrievePerPolling)['rows']
            logging.info("Found %i not archived documents from FWRJ db to upload to WMArchive.", len(data))

            for slicedData in grouper(data, self.numDocsUploadPerCall):
                jobIDs = []
                archiveDocs = []
                for job in slicedData:
                    doc = createArchiverDoc(job)
                    # check document size before accepting to send to WMArchive service
                    size = getSize(doc)
                    if size > self.sizeThreshold:
                        shortDoc = {'id': doc['id'],
                                    'fwjr': doc['doc']['fwjr'],
                                    'jobtype': doc['doc']['jobtype'],
                                    'jobstate': doc['doc']['jobstate']}
                        logging.warning("Created document is too large for WMArchive, size=%s thredshold=%s, document slice=%s", size, self.sizeThreshold, shortDoc)
                    else:
                        archiveDocs.append(doc)
                        jobIDs.append(job["id"])

                response = self.wmarchiver.archiveData(archiveDocs)

                # Partial success is not allowed either all the insert is successful or none is
                if response[0]['status'] == "ok" and len(response[0]['ids']) == len(jobIDs):
                    archiveIDs = response[0]['ids']
                    for docID in jobIDs:
                        self.fwjrAPI.updateArchiveUploadedStatus(docID)
                    logging.info("...successfully uploaded %d docs", len(jobIDs))
                    logging.debug("JobIDs uploaded: %s", jobIDs)
                    logging.debug("Archived IDs returned: %s", archiveIDs)
                else:
                    logging.warning("Upload failed and it will be retried in the next cycle: %s: %s.",
                                    response[0]['status'], response[0]['reason'])
                    logging.debug("failed JobIds %s", jobIDs)
        except Exception as ex:
            logging.error("Error occurred, will retry later:")
            logging.error(str(ex))
            logging.error("Trace back: \n%s", traceback.format_exc())
