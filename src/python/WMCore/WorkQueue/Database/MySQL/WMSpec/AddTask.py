"""
_New_

MySQL implementation of WMSpec.New
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class AddTask(DBFormatter):
    sql = """INSERT IGNORE INTO wq_wmtask (wmspec_id, name, type, dbs_url) VALUES (
                 (SELECT id FROM wq_wmspec WHERE name = :wmspec_name), 
                 :name, :type, :dbs_url)
          """

    def execute(self, wmSpecName, name, dbsUrl, type, conn = None, transaction = False):
        
        binds = {"wmspec_name": wmSpecName, "name": name, "type": type, "dbs_url": dbsUrl}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)            
        return