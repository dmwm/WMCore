#!/usr/bin/env python
"""
_InsertOutput_

MySQL implementation of Workflow.InsertOutput
"""

from WMCore.Database.DBFormatter import DBFormatter

class InsertOutput(DBFormatter):
    sql = """INSERT INTO wmbs_workflow_output (workflow_id, output_identifier,
                                               output_fileset, merged_output_fileset)
               SELECT :workflow AS workflow_id, :output AS output_identifier,
                 :fileset AS output_fileset, :mfileset AS merged_output_fileset
                 FROM DUAL WHERE NOT EXISTS
               (SELECT workflow_id FROM wmbs_workflow_output
                 WHERE :workflow = workflow_id AND
                       :output = output_identifier AND
                       :fileset = output_fileset)"""

    def execute(self, workflowID, outputIdentifier, filesetID, mergedFilesetID,
                conn = None, transaction = False):
        binds = {"workflow": workflowID, "output": outputIdentifier,
                 "fileset": filesetID, "mfileset": mergedFilesetID}
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
