import time
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Lexicon import splitCouchServiceURL

def monitorDocFromRequestSchema(schema):
    """
    prun and convert
    """
    doc = {}
    #from basic field in WMCore.RequestManager.DataStructs.RequestSchema
    doc['_id'] = schema['RequestName']
    doc['workflow'] = schema['RequestName']
    doc['requestor'] = schema['Requestor']
    doc['campaign'] = schema["Campaign"]
    doc['request_type'] = schema["RequestType"]
    doc['priority'] = schema["RequestPriority"]
    doc['group'] = schema["Group"]
    doc['request_date'] = schema["RequestDate"]
    
    # additional field
    doc['inputdataset'] = schema["InputDataset"]
    doc['team'] = schema['team']
    return doc

class WMStatsWriter():

    def __init__(self, couchURL, dbName = None):
        # set the connection for local couchDB call
        if dbName:
            self.couchURL = couchURL
            self.dbName = dbName
        else:
            self.couchURL, self.dbName = splitCouchServiceURL(couchURL)
        self.couchDB = CouchServer(self.couchURL).connectDatabase(self.dbName, False)

    def uploadData(self, docs):
        """
        upload to given couchURL using cert and key authentication and authorization
        """
        # add delete docs as well for the compaction
        # need to check whether delete and update is successful
        if type(docs) == str:
            docs = [docs]
        for doc in docs:
            self.couchDB.queue(doc)
        return self.couchDB.commit(returndocs = True)
    
    def insertRequest(self, schema):
        doc = monitorDocFromRequestSchema(schema)
        return self.couchDB.updateDocument(doc['_id'], 'WMStats', 
                                    'insertRequest', fields={'doc': doc})
    
    def updateRequestStatus(self, request, status):
        statusTime = [status, int(time.time())]
        return self.couchDB.updateDocument(request, 'WMStats', 'requestStatus', 
                                         fields={'request_status': statusTime})
    
    def insertTotalJobs(self, request, totalJobs):
        return self.couchDB.updateDocument(request, 'WMStats', 'requestStatus', 
                                         fields={'total_jobs': totalJobs})
        