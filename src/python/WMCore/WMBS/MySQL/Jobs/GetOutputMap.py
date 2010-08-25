#!/usr/bin/env python
"""
_GetOutputMap_

MySQL implementation of Jobs.GetOutputMap
"""

__revision__ = "$Id: GetOutputMap.py,v 1.1 2009/10/14 16:47:18 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetOutputMap(DBFormatter):
    """
    _GetOutputMap_

    """
    sql = """SELECT wmbs_workflow_output.output_identifier AS wf_output_id,
                    wmbs_workflow_output.output_fileset AS wf_output_fset,
                    output_subs.sub_type AS child_sub_type,
                    child_sub_output.output_identifier AS child_sub_output_id,
                    child_sub_output.output_fileset AS child_sub_output_fset
                    FROM wmbs_workflow_output
               INNER JOIN wmbs_subscription ON
                 wmbs_workflow_output.workflow_id = wmbs_subscription.workflow
               INNER JOIN wmbs_jobgroup ON
                 wmbs_subscription.id = wmbs_jobgroup.subscription
               INNER JOIN wmbs_job ON
                 wmbs_jobgroup.id = wmbs_job.jobgroup
               LEFT OUTER JOIN
                 (SELECT wmbs_subscription.id AS sub_id,
                         wmbs_subscription.fileset AS sub_fileset,
                         wmbs_subscription.workflow AS sub_workflow,
                         wmbs_sub_types.name AS sub_type
                         FROM wmbs_subscription
                    INNER JOIN wmbs_sub_types ON
                      wmbs_subscription.subtype = wmbs_sub_types.id) output_subs ON
                 wmbs_workflow_output.output_fileset = output_subs.sub_fileset
               LEFT OUTER JOIN
                 (SELECT * FROM wmbs_workflow_output) child_sub_output ON
                output_subs.sub_workflow = child_sub_output.workflow_id
             WHERE wmbs_job.id = :jobid"""

    def format(self, results):
        """
        _format_

        wf_output_id,
        wf_output_fset,
        child_sub_type,
        child_sub_output_id,
        child_sub_output_fset
        """
        results = self.formatDict(results)

        outputMap = {}
        for result in results:
            if not outputMap.has_key(result["wf_output_id"]):
                outputMap[result["wf_output_id"]] = {"fileset": None,
                                                     "children": []}

            labelMap = outputMap[result["wf_output_id"]]
            labelMap["fileset"] = result["wf_output_fset"]

            if result["child_sub_type"] != None:
                childDict = {"child_sub_type": result["child_sub_type"],
                             "child_sub_output_id": result["child_sub_output_id"],
                             "child_sub_output_fset": result["child_sub_output_fset"]}
                labelMap["children"].append(childDict)

        return outputMap

    def execute(self, jobID, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"jobid": jobID}, conn = conn,
                                      transaction = transaction)
        return self.format(result)
