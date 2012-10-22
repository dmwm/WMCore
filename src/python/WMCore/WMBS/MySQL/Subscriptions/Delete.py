#!/usr/bin/env python
"""
_Delete_

MySQL implementation of Subscriptions.Delete

"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class Delete(DBFormatter):
    """

    """
    sql = """DELETE FROM wmbs_subscription WHERE id = :id"""

    def getBinds(self, id=None):
        return self.dbi.buildbinds(self.dbi.makelist(id), 'id')

    def format(self, result):
        return True

    def execute(self, id = -1, conn = None, transaction = False):
        self.dbi.processData(self.sql, self.getBinds(id),
                     conn = conn, transaction = transaction)
        return True #or raise
