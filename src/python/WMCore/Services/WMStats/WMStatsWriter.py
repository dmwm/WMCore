import time
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Lexicon import splitCouchServiceURL
from WMCore.Wrappers.JsonWrapper import JSONEncoder

def monitorDocFromRequestSchema(schema):
    """
    prun and convert
    """
    doc = {}
    #from basic field in WMCore.RequestManager.DataStructs.RequestSchema
    doc["_id"] = schema['RequestName']
    doc["workflow"] = schema['RequestName']
    doc["requestor"] = schema['Requestor']
    doc["campaign"] = schema['Campaign']
    doc["request_type"] = schema['RequestType']
    doc["priority"] = schema['RequestPriority']
    doc["group"] = schema['Group']
    doc["request_date"] = schema['RequestDate']
    
    # additional field
    doc["inputdataset"] = schema.get('InputDataset', "")
    # team name is not yet available need to be updated in assign status
    #doc['team'] = schema['team']
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
        result = self.couchDB.updateDocument(doc['_id'], 'WMStats', 
                                    'insertRequest', fields={'doc': JSONEncoder().encode(doc)})
        self.updateRequestStatus(doc['_id'], "new")
        return result
    
    def updateRequestStatus(self, request, status):
        statusTime = {'status': status, 'update_time': int(time.time())}
        return self.couchDB.updateDocument(request, 'WMStats', 'requestStatus', 
                                         fields={'request_status': JSONEncoder().encode(statusTime)})
    
    def updateTeam(self, request, team):
        return self.couchDB.updateDocument(request, 'WMStats', 'team', 
                                         fields={'team': team})
        
    def insertTotalJobs(self, request, totalJobs):
        return self.couchDB.updateDocument(request, 'WMStats', 'totalJobs', 
                                         fields={'total_jobs': int(totalJobs)})
    
    def updateFromWMSpec(self, spec):
        # currently only update priority and siteWhitelist
        # complex field needs to be JSON encoded 
        # assuming all the toplevel tasks has the same site white lists
        #priority is priority + user priority + group priority
        fields = {'priority': spec.priority(), 'site_white_list': spec.getTopLevelTask()[0].siteWhitelist()}
        return self.couchDB.updateDocument(spec.name(), 'WMStats', 'generalFields', 
                                         fields={'general_fields': JSONEncoder().encode(fields)})
        