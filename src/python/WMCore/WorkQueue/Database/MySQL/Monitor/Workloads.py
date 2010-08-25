"""
WMCore/WorkQueue/Database/MySQL/Monitor/Workloads.py


DAO object for WorkQueue

"""

__all__ = []
__revision__ = "$Id: Workloads.py,v 1.1 2010/03/25 16:37:20 maxa Exp $"
__version__ = "$Revision: 1.1 $"


from WMCore.Database.DBFormatter import DBFormatter


class Workloads(DBFormatter):
    sql = """SELECT id, name, url, owner from wq_wmspec ORDER BY id"""
    
    def execute(self, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, conn = conn,
                                       transaction = transaction)
        
        return self.formatDict(results)