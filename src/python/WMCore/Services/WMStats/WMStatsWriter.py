import time
import logging
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Lexicon import splitCouchServiceURL, sanitizeURL
from WMCore.Wrappers.JsonWrapper import JSONEncoder
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader

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
    doc["type"] = "reqmgr_request"
    # additional field
    doc["inputdataset"] = schema.get('InputDataset', "")
    # additional field for Analysis work
    doc["vo_group"] = schema.get('VoGroup', "")
    doc["vo_role"] = schema.get('VoRole', "")
    doc["user_dn"] = schema.get('RequestorDN', "")
    doc["async_dest"] = schema.get('asyncDest', "")
    doc["dbs_url"] = schema.get("DbsUrl", "")
    doc["publish_dbs_url"] = schema.get("PublishDbsUrl", "")
    doc["outputdatasets"] = schema.get('OutputDatasets', [])
    doc["cmssw"] = schema.get('SoftwareVersions', [])
    doc['prep_id'] = schema.get('SoftwareVersions', None)

    # team name is not yet available need to be updated in assign status
    #doc['team'] = schema['team']
    return doc


class WMStatsWriter(WMStatsReader):

    def __init__(self, couchURL, dbName = None):
        # set the connection for local couchDB call
        if dbName:
            self.couchURL = couchURL
            self.dbName = dbName
        else:
            self.couchURL, self.dbName = splitCouchServiceURL(couchURL)
        self.couchServer = CouchServer(self.couchURL)
        self.couchDB = self.couchServer.connectDatabase(self.dbName, False)

    def uploadData(self, docs):
        """
        upload to given couchURL using cert and key authentication and authorization
        """
        # add delete docs as well for the compaction
        # need to check whether delete and update is successful
        if type(docs) == dict:
            docs = [docs]
        for doc in docs:
            self.couchDB.queue(doc)
        return self.couchDB.commit(returndocs = True)

    def insertRequest(self, schema):
        doc = monitorDocFromRequestSchema(schema)
        return self.insertGenericRequest(doc)

    def insertGenericRequest(self, doc):
        result = self.couchDB.updateDocument(doc['_id'], 'WMStats',
                                    'insertRequest',
                                    fields={'doc': JSONEncoder().encode(doc)})
        self.updateRequestStatus(doc['_id'], "new")
        return result

    def updateRequestStatus(self, request, status):
        statusTime = {'status': status, 'update_time': int(time.time())}
        return self.couchDB.updateDocument(request, 'WMStats', 'requestStatus',
                    fields={'request_status': JSONEncoder().encode(statusTime)})

    def updateTeam(self, request, team):
        return self.couchDB.updateDocument(request, 'WMStats', 'team',
                                         fields={'team': team})

    def insertTotalStats(self, request, totalStats):
        """
        update the total stats of given workflow (total_jobs, input_events, input_lumis, input_num_files)
        """
        return self.couchDB.updateDocument(request, 'WMStats', 'totalStats',
                                         fields=totalStats)

    def updateFromWMSpec(self, spec):
        # currently only update priority and siteWhitelist and output dataset
        # complex field needs to be JSON encoded
        # assuming all the toplevel tasks has the same site white lists
        #priority is priority + user priority + group priority
        fields = {'priority': spec.priority(),
                  'site_white_list': spec.getTopLevelTask()[0].siteWhitelist(),
                  'outputdatasets': spec.listOutputDatasets()}
        return self.couchDB.updateDocument(spec.name(), 'WMStats',
                    'generalFields',
                    fields={'general_fields': JSONEncoder().encode(fields)})

    def updateRequestsInfo(self, docs):
        """
        bulk update for request documents.
        TODO: change to bulk update handler when it gets supported
        """
        for doc in docs:
            del doc['type']
            self.couchDB.updateDocument(doc['workflow'], 'WMStats',
                        'generalFields',
                        fields={'general_fields': JSONEncoder().encode(doc)})

    def updateAgentInfo(self, agentInfo):
        return self.couchDB.updateDocument(agentInfo['_id'], 'WMStats',
                        'agentInfo',
                        fields={'agent_info': JSONEncoder().encode(agentInfo)})

    def deleteOldDocs(self, days):
        """
        delete the documents from wmstats db older than param 'days'
        """
        sec = int(days * 24 * 60 *60)
        threshold = int(time.time()) - sec
        options = {"startkey": threshold, "descending": True,
                   "stale": "update_after"}
        result = self.couchDB.loadView("WMStats", "time", options)

        for row in result['rows']:
            doc = {}
            doc['_id'] = row['value']['id']
            doc['_rev'] = row['value']['rev']
            self.couchDB.queueDelete(doc)
        committed = self.couchDB.commit()

        if committed:
            errorReport = {}
            deleted = 0
            for data in committed:
                if data.has_key('error'):
                    errorReport.setdefault(data['error'], 0)
                    errorReport[data['error']] += 1
                else:
                    deleted += 1
            return {'delete': deleted, 'error': errorReport}
        else:
            return "nothing"

    def replicate(self, target):
        return self.couchServer.replicate(self.dbName, target, continuous = True,
                                   filter = 'WMStats/repfilter', useReplicator = True)
    
    def getDBInstance(self):
        return self.couchDB

    def getServerInstance(self):
        return self.couchServer
    
    def getActiveTasks(self):
        couchStatus = self.couchServer.status()
        return couchStatus['active_tasks']
