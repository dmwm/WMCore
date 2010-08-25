"""
WMCore/WorkQueue/Database/SQLite/Monitor/WorkloadsByName.py

DAO object for WorkQueue.

WorkQueue database structure:
WMCore/WorkQueue/Database/CreateWorkQueueBase.py

"""

__all__ = []
__revision__ = "$Id: WorkloadsByName.py,v 1.1 2010/03/26 14:07:10 maxa Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter


class WorkloadsByName(DBFormatter):
    sql = """SELECT id, name, url, owner from wq_wmspec WHERE name = :name ORDER BY id"""

    
    def execute(self, name, conn = None, transaction = False):
        if type(name) != list:
            name = [name]
                
        bindVars = [{"name": i} for i in name]
                        
        results = self.dbi.processData(self.sql, bindVars, conn = conn,
                                      transaction = transaction)
                
        formResults = self.formatDict(results)
        return formResults