"""
API for querying the status of agent drain process
"""

from __future__ import division
from WMComponent.DBS3Buffer.DBSBufferUtil import DBSBufferUtil
from WMCore.Services.PyCondor.PyCondorAPI import PyCondorAPI


class DrainStatusAPI(object):
    """
    Provides methods for querying dbs and condor for drain statistics
    """
    def __init__(self):

        self.dbsUtil = DBSBufferUtil()
        self.condorAPI = PyCondorAPI()

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
        queries = [["1", "idle"], ["2", "running"]]

        for query in queries:
            jobs = self.condorAPI.getCondorJobs("JobStatus=="+query[0], [])
            # if there is an error, report it instead of the length of an empty list
            if jobs is None:
                results[query[1]] = "unknown (schedd query error)"
            else:
                results[query[1]] = len(jobs)

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
