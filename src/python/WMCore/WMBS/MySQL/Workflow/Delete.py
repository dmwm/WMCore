#!/usr/bin/env python
"""
_DeleteWorkflow_

MySQL implementation of DeleteWorkflow

"""
__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class Delete(DBFormatter):
    """
    Create a workflow ready for subscriptions
    """
    sql = """delete from wmbs_workflow where id = :id"""

    def getBinds(self, id=None):
        return self.dbi.buildbinds(self.dbi.makelist(id), 'id')

    def execute(self, id = -1, conn = None, transaction = False):
        self.dbi.processData(self.sql, self.getBinds(id),
                     conn = conn, transaction = transaction)
        return True #or raise
