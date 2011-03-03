#!/usr/bin/env python
"""
_UnmarkLocation_

MySQL implementation of WorkflowManager backend.
"""

from WMCore.Database.DBFormatter import DBFormatter

class UnmarkLocation(DBFormatter):

    sql = """DELETE FROM wm_managed_workflow_location
            WHERE managed_workflow = (SELECT id FROM wm_managed_workflow
                                       WHERE workflow = :workflow
                                       AND fileset_match = :fsmatch)
            AND location = (SELECT id FROM wmbs_location
                             WHERE site_name = :location)
"""

    def getBinds( self, workflow = '', fileset_match = '', location = '' ):
        """
        Bind parameters
        """
        dict = {'workflow' : workflow,
                'fileset_match': fileset_match,
                'location' : location }

        return dict

    def execute(self, workflow = '', fileset_match = '', location = '', conn = None, transaction = False):
        """
        Removes a location from the created subscription white / black lists
        """
        binds = self.getBinds( workflow, fileset_match, location )
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
