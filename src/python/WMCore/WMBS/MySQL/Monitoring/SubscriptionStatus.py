#!/usr/bin/env python
"""
_SubscriptionStatus_

Retrieve status information about subscriptions in WMBS.  This will return a
list of disctionaries with the following keys:
    percent_success
    percent_complete
    fileset_name
    workflow_name
    subscription_id

"""
from __future__ import division

from WMCore.Database.DBFormatter import DBFormatter

class SubscriptionStatus(DBFormatter):
    sql = """SELECT wmbs_subscription.id AS subscription_id,
                    wmbs_workflow.name AS workflow_name,
                    wmbs_fileset.name AS fileset_name,
                    job_info.job_state AS job_state,
                    job_info.job_count AS job_count,
                    job_info.num_success AS success_count FROM wmbs_subscription
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

    typeSql = """SELECT wmbs_subscription.id AS subscription_id,
                        wmbs_workflow.name AS workflow_name,
                        wmbs_fileset.name AS fileset_name,
                        job_info.job_state AS job_state,
                        job_info.job_count AS job_count,
                        job_info.num_success AS success_count FROM wmbs_subscription
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
            if result["workflow_name"] not in workflows:
                workflows[result["workflow_name"]] = {}

            workflowDict = workflows[result["workflow_name"]]
            if result["fileset_name"] not in workflowDict:
                workflowDict[result["fileset_name"]] = {"success": 0, "running": 0,
                                                       "failure": 0,
                                                       "subId": result["subscription_id"]}

            filesetDict = workflowDict[result["fileset_name"]]
            if result["job_state"] in ("exhausted", "cleanout", "success", "jobfailed"):
                if result["success_count"] != None:
                    filesetDict["success"] += result["success_count"]
                    filesetDict["failure"] += result["job_count"] - result["success_count"]
            else:
                if result["success_count"] != None:
                    filesetDict["running"] += result["job_count"]

        results = []
        for workflowName in workflows:
            for filesetName in workflows[workflowName]:
                success = workflows[workflowName][filesetName]["success"]
                failure = workflows[workflowName][filesetName]["failure"]
                running = workflows[workflowName][filesetName]["running"]

                if success + failure + running == 0:
                    percentComplete = 0
                    percentSuccess = 0
                elif success + failure == 0:
                    percentComplete = int((success + failure) / (success + failure + running) * 100)
                    percentSuccess = 0
                else:
                    percentComplete = int((success + failure) / (success + failure + running) * 100)
                    percentSuccess = int(success / (success + failure + running) * 100)

                subId = workflows[workflowName][filesetName]["subId"]

                results.append({"subscription_id": subId,
                                "workflow_name": workflowName,
                                "fileset_name": filesetName,
                                "percent_complete": percentComplete,
                                "percent_success": percentSuccess})
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
