"""
WMCore/WorkQueue/Database/MySQL/Monitor/Workloads.py


DAO object for WorkQueue

"""

__all__ = []




from WMCore.Database.DBFormatter import DBFormatter


class Workloads(DBFormatter):
    sql = """SELECT id, name, url, owner from wq_wmspec ORDER BY id"""
    
    def execute(self, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, conn = conn,
                                       transaction = transaction)
        
        return self.formatDict(results)