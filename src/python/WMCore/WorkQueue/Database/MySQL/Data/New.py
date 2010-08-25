"""
_New_

MySQL implementation of Block.New
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT INTO wq_data (name) VALUES (:name)"""

    def execute(self, name,
                conn = None, transaction = False):
        binds = {"name": name}
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
