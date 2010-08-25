#!/usr/bin/env python
"""
_New_

MySQL implementation of JobGroup.SetSite
"""

__all__ = []
__revision__ = "$Id: SetSite.py,v 1.1 2009/07/01 19:25:14 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class SetSite(DBFormatter):
    sql = """UPDATE wmbs_jobgroup jg
              SET jg.location = (SELECT ID FROM wmbs_location WHERE site_name = :site_name)
              WHERE jg.ID = :jobGroupID"""

    def execute(self, site_name = None, jobGroupID = None, conn = None,
                transaction = False):
        binds = {'site_name' : site_name, 'jobGroupID' : jobGroupID}
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return
