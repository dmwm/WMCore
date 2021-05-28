"""
API for querying the status of agent drain process
"""

from __future__ import division
from builtins import object

from WMComponent.DBS3Buffer.DBSBufferUtil import DBSBufferUtil
from WMCore.Services.PyCondor.PyCondorAPI import PyCondorAPI
from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend


class DrainStatusAPI(object):
    """
    Provides methods for querying dbs and condor for drain statistics
    """
    def __init__(self, config):
        # queue url used in WorkQueueManager
        self.thisAgentUrl = "http://" + config.Agent.hostName + ":5984"
        self.globalBackend = WorkQueueBackend(config.WorkloadSummary.couchurl)
        self.localBackend = WorkQueueBackend(config.WorkQueueManager.couchurl)
        self.dbsUtil = DBSBufferUtil()
        self.condorAPI = PyCondorAPI()
        self.condorStates = ("Running", "Idle")

    def collectDrainInfo(self):
        """
        Call methods to check the drain status
        """
        results = {}
        results['workflows_completed'] = self.checkWorkflows()

        # if workflows are completed, collect additional drain statistics
        if results['workflows_completed']:
            results['upload_status'] = self.checkFileUploadStatus()
            results['condor_status'] = self.checkCondorStates()
            results['local_wq_status'] = self.checkLocalWQStatus(dbname="workqueue")
            results['local_wqinbox_status'] = self.checkLocalWQStatus(dbname="workqueue_inbox")
            results['global_wq_status'] = self.checkGlobalWQStatus()

        return results

    def checkWorkflows(self):
        """
        Check to see if all workflows have a 'completed' status
        """
        results = self.dbsUtil.isAllWorkflowCompleted()
        return results

    def checkCondorStates(self):
        """
        Check idle and running jobs in Condor
        """
        results = {}
        jobs = self.condorAPI.getCondorJobsSummary()
        for state in self.condorStates:
            # if there is an error, report it instead of the length of an empty list
            if not jobs:
                results[state.lower()] = None
            else:
                results[state.lower()] = int(jobs[0].get(state))

        return results

    def checkFileUploadStatus(self):
        """
        Check file upload status:
            Blocks open in DBS
            Files not uploaded in DBS
            Files not uploaded to Phedex
        """
        results = {}
        results['dbs_open_blocks'] = self.dbsUtil.countOpenBlocks()
        results['dbs_notuploaded'] = self.dbsUtil.countFilesByStatus(status="NOTUPLOADED")
        results['phedex_notuploaded'] = self.dbsUtil.countPhedexNotUploaded()
        return results

    def checkLocalWQStatus(self, dbname):
        """
        Query local WorkQueue workqueue/workqueue_inbox database to see whether
        there are any active elements in this agent.
        """
        results = {}

        for st in ('Available', 'Negotiating', 'Acquired', 'Running'):
            if dbname == "workqueue":
                elements = self.localBackend.getElements(status=st, returnIdOnly=True)
            else:
                elements = self.localBackend.getInboxElements(status=st, returnIdOnly=True)
            results[st] = len(elements)
        return results

    def checkGlobalWQStatus(self):
        """
        Query Global WorkQueue workqueue database to see whether there are
        any active elements set to this agent.
        """
        results = {}

        for st in ("Acquired", "Running"):
            elements = self.globalBackend.getElements(status=st, returnIdOnly=True,
                                                      ChildQueueUrl=self.thisAgentUrl)
            results[st] = len(elements)
        return results
