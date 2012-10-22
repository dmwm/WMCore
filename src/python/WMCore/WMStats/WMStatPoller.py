
from WMCore.Database.CMSCouch import CouchServer
# Move this one under service
# need to use the lexicon instead of splitCouchServiceURL
def splitCouchServiceURL(serviceURL):
    """
    split service URL to couchURL and couchdb name
    serviceURL should be couchURL/dbname format.
    """

    splitedURL = serviceURL.rstrip('/').rsplit('/', 1)

    return splitedURL[0], splitedURL[1]

class WMStatSevice():

    def __init__(self, couchURL):
        # set the connection for local couchDB call
        self.couchURL, self.dbName = splitCouchServiceURL(couchURL)
        self.couchDB = CouchServer(self.couchURL).connectDatabase(self.dbName, False)

    def uploadData(self, docs):
        """
        upload to given couchURL using cert and key authentication and authorization
        """
        # add delete docs as well for the compaction
        # need to check whether delete and update is successful
        for doc in docs:
            self.couchDB.queue(doc)
        return self.couchDB.commit(returndocs = True)


# move these ones under Services/WorkQueue/WorkQueue.py
import time
def wqDataFormat(data):
    docs = []
    uploadTime = int(time.time())
    for item in data:
        doc = {}
        doc['timestamp'] = uploadTime
        doc['type'] = "gq_request"
        doc['workflow'] = item['request_name']
        doc['total_jobs'] = item['total_jobs']
        docs.append(doc)
    return docs

import time
def reqmgrDataFormat(data):
    docs = []
    uploadTime = int(time.time())
    for itemThunked in data:
        #print itemThunked
        item =  itemThunked['WMCore.RequestManager.DataStructs.Request.Request']
        doc = {}
        doc['timestamp'] = uploadTime
        doc['type'] = 'reqmgr_request'
        doc['requestor'] = item['Requestor']
        doc['group'] = item['Group']

        #TODO: Not yet supported
        #doc['team'] = item['Team']
        # user is requestor : double check
        #doc['user'] = item['Requestor']
        # Not yet supported
        #doc['campaign'] = item['Campaign']
        #doc['request_date'] = item['RequestDate']
        doc['workflow'] = item['RequestName']
        doc['request_type'] = item['RequestType']
        doc['request_status'] = item['RequestStatus']
        doc['input_datasets'] = item['InputDatasets']
        doc['acquisition_era'] = item['AcquisitionEra']
        doc['priority'] = item['RequestPriority']
        docs.append(doc)
    return docs

if __name__ == '__main__':
    import sys
    from optparse import OptionParser
    from WMCore.Configuration import loadConfigurationFile

    parser = OptionParser()
    parser.add_option("-i", "--ini", dest="inifile", default=False,
                      help="write the configuration to FILE", metavar="FILE")

    (opts, args) = parser.parse_args()


    if not opts.inifile:
        sys.exit('No configuration specified')
    cfg = loadConfigurationFile(opts.inifile)
    cfg = cfg.WMStats
    import cherrypy
    from WMCore.Services.RequestManager.RequestManager import RequestManager
    from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
    from WMCore.CherryPyThread.PeriodicWorker import PeriodicWorker
    from WMCore.WMStats.DataCollectTask import DataCollectTask
    import logging


    cherrypy.log.error_log.setLevel(logging.DEBUG)
    cherrypy.log.access_log.setLevel(logging.DEBUG)
    cherrypy.config["server.socket_port"] = cfg.port
    #def sayHello(test):
    #    print "Hello"
    #PeriodicWorker(sayHello, 5)

    # get reqmgr url from config
    reqmgrSvc = RequestManager({'endpoint': cfg.reqmgrURL})
    wqSvc = WorkQueue(cfg.globalQueueURL)
    wmstatSvc = WMStatSevice(cfg.couchURL)

    reqmgrTask = DataCollectTask(reqmgrSvc.getRequest,  reqmgrDataFormat, wmstatSvc.uploadData)
    #reqmgrTask = DataCollectTask(reqmgrSvc.getRequestNames,  lambda x: x, wmstatSvc.uploadData)

    #wqTask = DataCollectTask(wqSvc.getTopLevelJobsByRequest, wqDataFormat, wmstatSvc.uploadData)

    reqmgrWorker = PeriodicWorker(reqmgrTask, cfg.pollInterval)
    #wqWorker = PeriodicWorker(wqTask, 200)

    cherrypy.quickstart()
