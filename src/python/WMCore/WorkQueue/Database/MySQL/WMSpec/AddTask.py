"""
_New_

MySQL implementation of WMSpec.New
"""

__all__ = []
__revision__ = "$Id: AddTask.py,v 1.1 2009/11/20 22:59:57 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class AddTask(DBFormatter):
    sql = """INSERT IGNORE INTO wq_wmtask (wmspec_id, name, dbs_url) VALUES (
                 (SELECT id FROM wq_wmspec WHERE name = :wmspec_name), 
                 :name, :dbs_url)
          """

    def execute(self, wmSpecName, name, dbsUrl, conn = None, transaction = False):
        
        binds = {"wmspec_name": wmSpecName, "name": name, "dbs_url": dbsUrl}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)            
        return