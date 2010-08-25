"""
_New_

MySQL implementation of Site.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.3 2009/08/18 23:18:15 swakef Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT IGNORE INTO wq_site (name) VALUES (:name)"""

    def execute(self, names, conn = None, transaction = False):
        binds = [{"name": name} for name in names]

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
