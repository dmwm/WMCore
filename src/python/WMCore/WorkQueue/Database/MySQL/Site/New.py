"""
_New_

MySQL implementation of Site.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.1 2009/06/10 21:07:15 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT INTO wq_site (name) VALUES (:name)
          """

    def execute(self, name, conn = None, transaction = False):
        binds = {"name": name}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)            
        return
