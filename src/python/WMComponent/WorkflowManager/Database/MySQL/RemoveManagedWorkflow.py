#!/usr/bin/env python
"""
_RemoveManagedWorkflow_

MySQL implementation of WorkflowManager backend.
"""

from WMCore.Database.DBFormatter import DBFormatter

class RemoveManagedWorkflow(DBFormatter):

    sql = """DELETE FROM wm_managed_workflow
                    WHERE workflow = :workflow
                    AND fileset_match = :fileset_match
                    """

    def getBinds( self, workflow = '', fileset_match = '' ):
        """
        Bind parameters
        """
        dict = {'workflow' : workflow,
                'fileset_match': fileset_match }

        return dict

    def execute(self, workflow = '', fileset_match = '', conn = None, transaction = False):
        """
        Removes a workflow from management
        """
        binds = self.getBinds( workflow, fileset_match )
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
