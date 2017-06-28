#!/usr/bin/env python
"""
_WorkUnit.Delete_

MySQL implementation of WorkUnit.Delete
"""

from __future__ import absolute_import, division, print_function

from WMCore.Database.DBFormatter import DBFormatter


class Delete(DBFormatter):
    """
    _WorkUnit.Delete_

    MySQL implementation of WorkUnit.Delete
    """

    sql = 'DELETE FROM wmbs_workunit WHERE id = :id'

    def getBinds(self, name=None):
        return self.dbi.buildbinds(self.dbi.makelist(name), 'id')

    def execute(self, wuid=None, conn=None, transaction=False):
        self.dbi.processData(self.sql, self.getBinds(wuid), conn=conn, transaction=transaction)
        return True
