import logging
import random
import time
from json import JSONEncoder

import WMCore
from Utils.IteratorTools import grouper
from WMCore.Database.CMSCouch import CouchNotFoundError
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader


def monitorDocFromRequestSchema(schema):
    """
    prun and convert
    """
    doc = {}
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
    doc["cmssw"] = schema.get('CMSSWVersion', [])
    doc['prep_id'] = schema.get('PrepID', None)

    # team name is not yet available need to be updated in assign status
    # doc['team'] = schema['team']
    return doc


def convertToServiceCouchDoc(wqInfo, wqURL):
    """
    Convert services generic info into a proper couch doc.
    """
    wqDoc = {}
    wqDoc['_id'] = wqURL
    wqDoc['agent_url'] = wqURL
    wqDoc['agent_team'] = ""
    wqDoc['agent_version'] = WMCore.__version__
    wqDoc['timestamp'] = int(time.time())
    wqDoc['down_components'] = []
    wqDoc['type'] = "agent_info"
    wqDoc.update(wqInfo)

    return wqDoc


class WMStatsWriter(WMStatsReader):
    def __init__(self, couchURL, appName="WMStats", reqdbURL=None, reqdbCouchApp="ReqMgr"):
        # set the connection for local couchDB call
        WMStatsReader.__init__(self, couchURL, appName, reqdbURL, reqdbCouchApp)

    def _sanitizeURL(self, couchURL):
        """
        don't sanitize url for writer
        """
        return couchURL

    def uploadData(self, docs):
        """
        upload to given couchURL using cert and key authentication and authorization
        """
        # add delete docs as well for the compaction
        # need to check whether delete and update is successful
        if isinstance(docs, dict):
            docs = [docs]
        for doc in docs:
            self.couchDB.queue(doc)
        return self.couchDB.commit(returndocs=True)

    def bulkUpdateData(self, docs, existingDocs):
        """
        Update documents to WMStats in bulk, breaking down to 100 docs chunks.
        :param docs: docs to insert or update
        :param existingDocs: dict of docId: docRev of docs already existent in wmstats
        """
        if isinstance(docs, dict):
            docs = [docs]
        for chunk in grouper(docs, 100):
            for doc in chunk:
                if doc['_id'] in existingDocs:
                    revList = existingDocs[doc['_id']].split('-')
                    # update the revision number and keep the history of the revision
                    doc['_revisions'] = {"start": int(revList[0]) + 1, "ids": [str(int(revList[1]) + 1), revList[1]]}
                else:
                    # then create a random 10 digits uid for the first revision number, required by new_edits=False
                    firstId = "%10d" % random.randrange(9999999999)
                    doc['_revisions'] = {"start": 1, "ids": [firstId]}
                self.couchDB.queue(doc)

            logging.info("Committing bulk of %i docs ...", len(chunk))
            self.couchDB.commit(new_edits=False)
        return

    def insertRequest(self, schema):
        doc = monitorDocFromRequestSchema(schema)
        return self.insertGenericRequest(doc)

    def updateTeam(self, request, team):
        return self.couchDB.updateDocument(request, self.couchapp, 'team',
                                           fields={'team': team})

    def insertTotalStats(self, request, totalStats):
        """
        update the total stats of given workflow (total_jobs, input_events, input_lumis, input_num_files)
        """
        return self.couchDB.updateDocument(request, self.couchapp, 'totalStats',
                                           fields=totalStats)

    def updateFromWMSpec(self, spec):
        # currently only update priority and siteWhitelist and output dataset
        # complex field needs to be JSON encoded
        # assuming all the toplevel tasks has the same site white lists
        # priority is priority + user priority + group priority
        fields = {'priority': spec.priority(),
                  'site_white_list': spec.getTopLevelTask()[0].siteWhitelist(),
                  'outputdatasets': spec.listOutputDatasets()}
        return self.couchDB.updateDocument(spec.name(), self.couchapp,
                                           'generalFields',
                                           fields={'general_fields': JSONEncoder().encode(fields)})

    def updateRequestsInfo(self, docs):
        """
        bulk update for request documents.
        TODO: change to bulk update handler when it gets supported
        """
        for doc in docs:
            del doc['type']
            self.couchDB.updateDocument(doc['workflow'], self.couchapp,
                                        'generalFields',
                                        fields={'general_fields': JSONEncoder().encode(doc)})

    def updateAgentInfo(self, agentInfo, propertiesToKeep=None):
        """
        replace the agentInfo document with new one.
        :param agentInfo: dictionary for agent info
        :param propertiesToKeep: list of properties to keep original value
        :return: None
        """
        try:
            exist_doc = self.couchDB.document(agentInfo["_id"])
            agentInfo["_rev"] = exist_doc["_rev"]
            if propertiesToKeep and isinstance(propertiesToKeep, list):
                for prop in propertiesToKeep:
                    if prop in exist_doc:
                        agentInfo[prop] = exist_doc[prop]

        except CouchNotFoundError:
            # this means document is not exist so we will just insert
            pass
        finally:
            result = self.couchDB.commitOne(agentInfo)
        return result

    def updateAgentInfoInPlace(self, agentURL, agentInfo):
        """
        :param agentInfo: dictionary for agent info
        :return: document update status

        update agentInfo in couch in place without replacing a doucment
        """
        return self.couchDB.updateDocument(agentURL, self.couchapp, 'agentInfo', fields=agentInfo)

    def updateLogArchiveLFN(self, jobNames, logArchiveLFN):
        for jobName in jobNames:
            self.couchDB.updateDocument(jobName, self.couchapp,
                                        'jobLogArchiveLocation',
                                        fields={'logArchiveLFN': logArchiveLFN})

    def deleteOldDocs(self, days):
        """
        delete the documents from wmstats db older than param 'days'
        """
        sec = int(days * 24 * 60 * 60)
        threshold = int(time.time()) - sec
        options = {"startkey": threshold, "descending": True,
                   "stale": "update_after"}
        result = self._getCouchView("time", options)

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
                if 'error' in data:
                    errorReport.setdefault(data['error'], 0)
                    errorReport[data['error']] += 1
                else:
                    deleted += 1
            return {'delete': deleted, 'error': errorReport}
        else:
            return "nothing"

    def replicate(self, target):
        return self.couchServer.replicate(self.dbName, target, continuous=True,
                                          filter='WMStats/repfilter')

    def getActiveTasks(self):
        """
        This is in Writter instance since it needs admin permission
        """
        couchStatus = self.couchServer.status()
        return couchStatus['active_tasks']

    def deleteDocsByIDs(self, ids):

        if not ids:
            # if ids is empty don't run this
            # it will delete all the docs
            return None

        docs = self.couchDB.allDocs(keys=ids)['rows']
        for j in docs:
            doc = {}
            doc["_id"] = j['id']
            doc["_rev"] = j['value']['rev']
            self.couchDB.queueDelete(doc)
        committed = self.couchDB.commit()
        return committed

    def replaceRequestTransitionFromReqMgr(self, docs):
        """
        bulk update for request documents.
        TODO: change to bulk update handler when it gets supported
        """

        for doc in docs:
            requestName = doc["RequestName"]
            requestTransition = {}
            requestTransition['request_status'] = []
            for r in doc["RequestTransition"]:
                newR = {}
                newR['status'] = r['Status']
                newR['update_time'] = r['UpdateTime']
                requestTransition['request_status'].append(newR)

            self.couchDB.updateDocument(requestName, self.couchapp,
                                        'generalFields',
                                        fields={'general_fields': JSONEncoder().encode(requestTransition)})

    def deleteDocsByWorkflow(self, requestName):
        """
        delete all wmstats docs for a given requestName
        """
        options = {"reduce": False, "key": requestName}
        docs = self._getCouchView("allWorkflows", options)['rows']

        for j in docs:
            doc = {}
            doc["_id"] = j['value']['id']
            doc["_rev"] = j['value']['rev']
            self.couchDB.queueDelete(doc)
        committed = self.couchDB.commit()
        return committed
