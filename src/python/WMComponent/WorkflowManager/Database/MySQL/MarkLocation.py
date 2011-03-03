#!/usr/bin/env python
"""
_MarkLocation_

MySQL implementation of WorkflowManager backend.
"""

from WMCore.Database.DBFormatter import DBFormatter

class MarkLocation(DBFormatter):

    sql = """
           INSERT INTO wm_managed_workflow_location (managed_workflow, location,
                                                      valid)
           VALUES ((SELECT id from wm_managed_workflow
                     WHERE workflow = :workflow AND fileset_match = :fsmatch),
                   (SELECT id FROM wmbs_location WHERE site_name = :location),
                   :valid);
"""

    def getBinds( self, workflow = '', fileset_match = '', location = '', valid = '' ):
        """
        Bind parameters
        """
        dict = {'workflow' : workflow,
                'fsmatch': fileset_match,
                'location' : location,
                'valid' : valid }

        return dict

    def execute(self, workflow = '', fileset_match = '', location = '', valid = '', conn = None, transaction = False):
        """
        Mark location
        """
        binds = self.getBinds( workflow, fileset_match, location, valid )
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
