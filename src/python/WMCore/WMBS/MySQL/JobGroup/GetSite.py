#!/usr/bin/env python
"""
_New_

MySQL implementation of JobGroup.SetSite
"""

__all__ = []
__revision__ = "$Id: GetSite.py,v 1.1 2009/07/01 19:27:51 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetSite(DBFormatter):
    sql = """SELECT site_name FROM wmbs_location
              WHERE ID = (SELECT location FROM wmbs_jobgroup jg WHERE jg.ID = :jobGroupID)"""


    def format(self, result):
        rowProxy = result[0].fetchall()[0]
        return rowProxy.values()[0]

    def execute(self, jobGroupID = None, conn = None,
                transaction = False):
        binds = {'jobGroupID' : jobGroupID}
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return self.format(result)
