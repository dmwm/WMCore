"""
_New_

MySQL implementation of Site.New
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT IGNORE INTO wq_site (name) VALUES (:name)"""

    def execute(self, names, conn = None, transaction = False):
        binds = [{"name": name} for name in names]

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
