"""
_New_

MySQL implementation of WMSpec.New
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT INTO wq_wmspec (name, url, owner) VALUES (:name, :url, :owner)
          """

    def execute(self, name, url, owner, conn = None, transaction = False):
        binds = {"name": name, "url": url, "owner": owner}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)            
        return
