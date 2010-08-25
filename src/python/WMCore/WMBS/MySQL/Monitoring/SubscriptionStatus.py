#!/usr/bin/env python
"""
_SubscriptionStatus_

Retrieve status information about subscriptions in WMBS.  This will return a
list of disctionaries with the following keys:
    success
    failure
    running
    filesetName
    workflowName
    subscriptionId

The success, failure and running keys will hold information about the percentage
of successful, failed and running jobs.  The workflowName, filesetName and
subscriptionId keys will contain the workflow name, fileset name and WMBS
subscription IDs.
"""

__revision__ = "$Id: SubscriptionStatus.py,v 1.1 2009/11/17 18:33:47 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class SubscriptionStatus(DBFormatter):
    sql = """SELECT wmbs_subscription.id AS subscriptionid,
                    wmbs_workflow.name AS workflowname,
                    wmbs_fileset.name AS filesetname,
                    job_info.job_state AS jobstate,
                    job_info.job_count AS jobcount,
                    job_info.num_success AS successCount FROM wmbs_subscription
               INNER JOIN wmbs_workflow ON
                 wmbs_subscription.workflow = wmbs_workflow.id
               INNER JOIN wmbs_fileset ON
                 wmbs_subscription.fileset = wmbs_fileset.id
               LEFT OUTER JOIN
                 (SELECT wmbs_jobgroup.subscription AS subscription,
                         COUNT(*) AS job_count,
                         SUM(wmbs_job.outcome) AS num_success,
                         wmbs_job_state.name AS job_state FROM wmbs_jobgroup
                    INNER JOIN wmbs_job ON
                      wmbs_jobgroup.id = wmbs_job.jobgroup
                    INNER JOIN wmbs_job_state ON
                      wmbs_job.state = wmbs_job_state.id
                  GROUP BY wmbs_jobgroup.subscription, wmbs_job_state.name) job_info ON
                 wmbs_subscription.id = job_info.subscription"""

    typeSql = """SELECT wmbs_subscription.id AS subscriptionId,
                        wmbs_workflow.name AS workflowName,
                        wmbs_fileset.name AS filesetName,
                        job_info.job_state AS jobState,
                        job_info.job_count AS jobCount,
                        job_info.num_success AS successCount FROM wmbs_subscription
                   INNER JOIN wmbs_workflow ON
                     wmbs_subscription.workflow = wmbs_workflow.id
                   INNER JOIN wmbs_fileset ON
                     wmbs_subscription.fileset = wmbs_fileset.id
                   INNER JOIN wmbs_sub_types ON
                     wmbs_subscription.subtype = wmbs_sub_types.id
                   LEFT OUTER JOIN
                     (SELECT wmbs_jobgroup.subscription AS subscription,
                             COUNT(*) AS job_count,
                             SUM(wmbs_job.outcome) AS num_success,
                             wmbs_job_state.name AS job_state FROM wmbs_jobgroup
                        INNER JOIN wmbs_job ON
                          wmbs_jobgroup.id = wmbs_job.jobgroup
                        INNER JOIN wmbs_job_state ON
                          wmbs_job.state = wmbs_job_state.id
                      GROUP BY wmbs_jobgroup.subscription, wmbs_job_state.name) job_info ON
                     wmbs_subscription.id = job_info.subscription
                 WHERE wmbs_sub_types.name = :subscriptionType"""
    
    def format(self, result):
        """
        _format_

        Format the results of the query into something reasonable.  The query
        will return a row for each subscription/job type combination so we need
        to condense that down and return a single dictionary with information on
        job status for each subscription.
        """
        results = DBFormatter.formatDict(self, result)

        workflows = {}
        for result in results:
            if not workflows.has_key(result["workflowname"]):
                workflows[result["workflowname"]] = {}

            workflowDict = workflows[result["workflowname"]]
            if not workflowDict.has_key(result["filesetname"]):
                workflowDict[result["filesetname"]] = {"success": 0, "running": 0,
                                                       "failure": 0,
                                                       "subId": result["subscriptionid"]}

            filesetDict = workflowDict[result["filesetname"]]
            if result["jobstate"] in ("exhausted", "cleanout", "success"):
                if result["successcount"] != None:
                    filesetDict["success"] += result["successcount"]
                    filesetDict["failure"] += result["jobcount"] - result["successcount"]
            else:
                if result["successcount"] != None:
                    filesetDict["running"] += result["successcount"]
                    
        results = []
        for workflowName in workflows.keys():
            for filesetName in workflows[workflowName].keys():
                totalSuccess = workflows[workflowName][filesetName]["success"]
                totalFailure = workflows[workflowName][filesetName]["failure"]
                totalRunning = workflows[workflowName][filesetName]["running"]
                subId = workflows[workflowName][filesetName]["subId"]                
                totalJobs = totalSuccess + totalFailure + totalRunning

                if totalJobs == 0:
                    success = "No jobs defined."
                    failure = "No jobs defined."
                    running = "No jobs defined."
                else:
                    success = "%.2f%% (%d / %d)" % (100.0 * (totalSuccess / totalJobs),
                                                    totalSuccess, totalJobs)
                    failure = "%.2f%% (%d / %d)" % (100.0 * (totalFailure / totalJobs),
                                                    totalFailure, totalJobs)
                    running = "%.2f%% (%d / %d)" % (100.0 * (totalRunning / totalJobs),
                                                    totalRunning, totalJobs)
                    
                results.append({"subscriptionId": subId,
                                "workflowName": workflowName,
                                "filesetName": filesetName,
                                "success": success,
                                "failure": failure,
                                "running": running})
                                
        return results
        
    def execute(self, subscriptionType = None, conn = None,
                transaction = False):
        if subscriptionType == None:
            result = self.dbi.processData(self.sql, conn = conn,
                                          transaction = transaction)
        else:
            bindVars = {"subscriptionType": subscriptionType}
            result = self.dbi.processData(self.typeSql, bindVars, conn = conn,
                                          transaction = transaction)
            
        return self.format(result)
