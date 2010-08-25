"""
WMCore/WorkQueue/Database/SQLite/Monitor/WorkloadsById

DAO object for WorkQueue.

WorkQueue database structure:
WMCore/WorkQueue/Database/CreateWorkQueueBase.py

"""

__all__ = []



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