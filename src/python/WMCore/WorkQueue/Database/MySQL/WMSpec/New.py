"""
_New_

MySQL implementation of WMSpec.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.2 2009/06/24 21:00:24 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT INTO wq_wmspec (name, url) VALUES (:name, :url)
          """

    def execute(self, name, url, conn = None, transaction = False):
        binds = {"name": name, "url": url}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)            
        return
