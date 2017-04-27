#!/usr/bin/env python
"""
_WorkUnit.ExistsByID_

MySQL implementation of WorkUnit.ExistsByID
"""

from __future__ import absolute_import, division, print_function

from WMCore.Database.DBFormatter import DBFormatter


class ExistsByID(DBFormatter):
    """
    _WorkUnit.ExistsByID_

    MySQL implementation of WorkUnit.ExistsByID
    """

    sql = 'SELECT id FROM wmbs_workunit WHERE id = :id'

    def format(self, result):
        result = DBFormatter.format(self, result)
        if result:
            return result[0][0]
        else:
            return False

    def getBinds(self, wuid):
        return self.dbi.buildbinds(self.dbi.makelist(wuid), "id")

    def execute(self, wuid, conn=None, transaction=False):
        result = self.dbi.processData(self.sql, self.getBinds(wuid), conn=conn, transaction=transaction)
        return self.format(result)
