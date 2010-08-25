"""
_Exists_

MySQL implementation of WMSpec.Exists
"""
__all__ = []
__revision__ = "$Id: ExistsTask.py,v 1.1 2010/08/06 20:45:38 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class ExistsTask(DBFormatter):
    
    sql = """SELECT wt.id FROM wq_wmtask wt 
                 INNER JOIN wq_wmspec ws ON ws.id = wt.wmspec_id
                 WHERE wt.name = :name AND ws.name = :wmspec_name"""
    
    def format(self, result):
        result = DBFormatter.format(self, result)
        if len(result) == 0:
            return False
        else:
            return int(result[0][0])
        
    def execute(self, wmSpecName, name, conn = None, transaction = False):
        
        binds = {"wmspec_name": wmSpecName, "name": name}
        result = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction) 
        
        return self.format(result)