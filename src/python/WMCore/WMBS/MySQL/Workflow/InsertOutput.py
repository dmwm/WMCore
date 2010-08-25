#!/usr/bin/env python
"""
_InsertOutput_

MySQL implementation of Workflow.InsertOutput
"""

__all__ = []
__revision__ = "$Id: InsertOutput.py,v 1.1 2009/04/01 18:47:28 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class InsertOutput(DBFormatter):
    sql = """INSERT INTO wmbs_workflow_output (workflow_id, output_identifier,
                                               output_fileset)
             VALUES (:workflow, :output, :fileset)"""
    
    def execute(self, workflowID, outputIdentifier, filesetID, conn = None,
                transaction = False):
        binds = {"workflow": workflowID,
                 "output": outputIdentifier,
                 "fileset": filesetID}
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
