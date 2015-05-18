#!/usr/bin/env python
"""
_GetOutputMap_

MySQL implementation of Jobs.GetOutputMap
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetOutputMap(DBFormatter):
    sql = """SELECT wmbs_workflow_output.output_identifier AS wf_output_id,
                    wmbs_workflow_output.output_fileset AS wf_output_fset,
                    wmbs_workflow_output.merged_output_fileset AS wf_output_mfset
                    FROM wmbs_workflow_output
               INNER JOIN wmbs_subscription ON
                 wmbs_workflow_output.workflow_id = wmbs_subscription.workflow
               INNER JOIN wmbs_jobgroup ON
                 wmbs_subscription.id = wmbs_jobgroup.subscription
               INNER JOIN wmbs_job ON
                 wmbs_jobgroup.id = wmbs_job.jobgroup
             WHERE wmbs_job.id = :jobid"""

    def execute(self, jobID, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, {"jobid": jobID}, conn = conn,
                                       transaction = transaction)

        outputMap = {}
        for result in self.formatDict(results):
            if result["wf_output_id"] not in outputMap:
                outputMap[result["wf_output_id"]] = []

            outputMap[result["wf_output_id"]].append({"output_fileset": result["wf_output_fset"],
                                                      "merged_output_fileset": result["wf_output_mfset"]})
        return outputMap
