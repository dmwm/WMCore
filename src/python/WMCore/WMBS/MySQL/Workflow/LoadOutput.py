#!/usr/bin/env python
"""
_LoadOutput_

MySQL implementation of Workflow.LoadOutput
"""

from WMCore.Database.DBFormatter import DBFormatter

class LoadOutput(DBFormatter):
    sql = """SELECT output_identifier, output_fileset FROM wmbs_workflow_output
             WHERE workflow_id = :workflow"""

    def formatDict(self, result):
        """
        _formatDict_

        Cast the fileset id attribute to an int because the DBFormatter turns
        everything into strings.
        """
        tempResults = DBFormatter.formatDict(self, result)

        formattedResults = []
        for tempResult in tempResults:
            tempResult["output_fileset"] = int(tempResult["output_fileset"])
            formattedResults.append(tempResult)

        return formattedResults
                                    
    def execute(self, workflow, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"workflow": workflow}, 
                         conn = conn, transaction = transaction)
        return self.formatDict(result)
