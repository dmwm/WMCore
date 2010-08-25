"""
WMCore/WorkQueue/Database/SQLite/Monitor/WorkloadsByOwner.py

DAO object for WorkQueue.

WorkQueue database structure:
WMCore/WorkQueue/Database/CreateWorkQueueBase.py

"""

__all__ = []
__revision__ = "$Id: WorkloadsByOwner.py,v 1.1 2010/04/12 20:54:10 maxa Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter


class WorkloadsByOwner(DBFormatter):
    sql = """SELECT id, name, url, owner from wq_wmspec WHERE owner = :owner ORDER BY id"""

    
    def execute(self, owner, conn = None, transaction = False):
        if type(owner) != list:
            owner = [owner]
                
        bindVars = [{"owner": i} for i in owner]
                        
        results = self.dbi.processData(self.sql, bindVars, conn = conn,
                                      transaction = transaction)
                
        formResults = self.formatDict(results)
        return formResults