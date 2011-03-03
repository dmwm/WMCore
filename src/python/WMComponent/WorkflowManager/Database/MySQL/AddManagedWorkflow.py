#!/usr/bin/env python
"""
_AddManagedWorkflow_

MySQL implementation of WorkflowManager backend.
"""

from WMCore.Database.DBFormatter import DBFormatter
import logging

class AddManagedWorkflow(DBFormatter):

    sql = """INSERT INTO
wm_managed_workflow(workflow, fileset_match, split_algo, type)
VALUES (:workflow, :fileset_match, :split_algo, :type)
"""

    def getBinds( self, workflow = '', fileset_match = '', split_algo = '', type = '' ):
        """
        Bind parameters
        """
        dict = {'workflow' : workflow,
                'fileset_match': fileset_match,
                'split_algo' : split_algo,
                'type' : type }

        return dict

    def execute(self, workflow = '', fileset_match = '', split_algo = '', type = '', conn = None, transaction = False):
        """
        Add a workflow for management
        """
        binds = self.getBinds( workflow, fileset_match, split_algo, type )
        self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
        return
