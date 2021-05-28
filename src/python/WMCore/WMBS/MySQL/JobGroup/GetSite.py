#!/usr/bin/env python
"""
_New_

MySQL implementation of JobGroup.SetSite
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

from future.utils import listvalues

class GetSite(DBFormatter):
    sql = """SELECT site_name FROM wmbs_location
              WHERE ID = (SELECT location FROM wmbs_jobgroup jg WHERE jg.ID = :jobGroupID)"""


    def format(self, result):
        rowProxy = result[0].fetchall()[0]
        return listvalues(rowProxy)[0]

    def execute(self, jobGroupID = None, conn = None,
                transaction = False):
        binds = {'jobGroupID' : jobGroupID}
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return self.format(result)
