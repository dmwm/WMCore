"""
_New_

MySQL implementation of Block.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.1 2009/09/03 15:44:18 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT IGNORE INTO wq_data (name) VALUES (:name)"""

    def execute(self, name,
                conn = None, transaction = False):
        binds = {"name": name}
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
