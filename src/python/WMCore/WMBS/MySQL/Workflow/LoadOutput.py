#!/usr/bin/env python
"""
_LoadOutput_

MySQL implementation of Workflow.LoadOutput
"""

from WMCore.Database.DBFormatter import DBFormatter

class LoadOutput(DBFormatter):
    sql = """SELECT output_identifier AS wf_output_id,
                    output_fileset AS wf_output_fset,
                    merged_output_fileset AS wf_output_mfset
                    FROM wmbs_workflow_output
             WHERE workflow_id = :workflow"""

    def execute(self, workflow, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, {"workflow": workflow},
                                       conn = conn, transaction = transaction)

        outputMap = {}
        for result in self.formatDict(results):
            if result["wf_output_id"] not in outputMap:
                outputMap[result["wf_output_id"]] = []

            outputMap[result["wf_output_id"]].append({"output_fileset": result["wf_output_fset"],
                                                      "merged_output_fileset": result["wf_output_mfset"]})
        return outputMap
