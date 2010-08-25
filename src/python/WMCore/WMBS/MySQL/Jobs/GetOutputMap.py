#!/usr/bin/env python
"""
_GetOutputMap_

MySQL implementation of Jobs.GetOutputMap
"""

__revision__ = "$Id: GetOutputMap.py,v 1.2 2009/12/17 21:54:08 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetOutputMap(DBFormatter):
    """
    _GetOutputMap_

    """
    sql = """SELECT wmbs_workflow_output.output_identifier AS wf_output_id,
                    wmbs_workflow_output.output_fileset AS wf_output_fset,
                    wmbs_workflow_output.output_parent AS wf_output_parent,
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

        Format the result of query into something useful.  Results are returned
        in the form of a dictionary keyed by the workflow's output identifier
        (output module label).  Each value will be a dictionary with the
        following keys:
          fileset - The ID of the fileset for the output module
          parent - The parent of the output module.  This will be the name of
            another output module (for redneck parentage).  Most of the time it
            will be null and the input files for the job will be set as parents.
          children - A list of dictionaries with infomration on any
            subscriptions that run over the output fileset.  Each value will be
            a dictionary with the following keys:
              child_sub_type - Child subscription type
              child_sub_output_id - Child subscription output identifier
              child_sub_output_fset - Child susbcription output fileset

        Note that the child subscription information is included to facilitate
        files that go straight to merged.
        """
        results = self.formatDict(results)

        outputMap = {}
        for result in results:
            if not outputMap.has_key(result["wf_output_id"]):
                outputMap[result["wf_output_id"]] = {"fileset": None,
                                                     "parent": result["wf_output_parent"],
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
