"""
_New_

MySQL implementation of Block.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.2 2010/08/06 20:45:37 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT INTO wq_data (name) VALUES (:name)"""

    def execute(self, name,
                conn = None, transaction = False):
        binds = {"name": name}
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
