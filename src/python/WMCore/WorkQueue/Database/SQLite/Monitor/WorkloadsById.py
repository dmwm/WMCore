"""
WMCore/WorkQueue/Database/SQLite/Monitor/WorkloadsById

DAO object for WorkQueue.

WorkQueue database structure:
WMCore/WorkQueue/Database/CreateWorkQueueBase.py

"""

__all__ = []
__revision__ = "$Id: WorkloadsById.py,v 1.1 2010/03/26 14:07:10 maxa Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter


class WorkloadsById(DBFormatter):
    sql = """SELECT id, name, url, owner from wq_wmspec WHERE id = :id ORDER BY id """
    
    
    def execute(self, id, conn = None, transaction = False):
        if type(id) != list:
            id = [id]
                
        bindVars = [{"id": i} for i in id]
                
        results = self.dbi.processData(self.sql, bindVars, conn = conn,
                                      transaction = transaction)
                
        formResults = self.formatDict(results)
        return formResults