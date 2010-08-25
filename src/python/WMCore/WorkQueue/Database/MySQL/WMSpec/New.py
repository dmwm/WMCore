"""
_New_

MySQL implementation of WMSpec.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.4 2009/11/20 22:59:57 sryu Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT INTO wq_wmspec (name, url, owner) VALUES (:name, :url, :owner)
          """

    def execute(self, name, url, owner, conn = None, transaction = False):
        binds = {"name": name, "url": url, "owner": owner}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)            
        return
