"""
_Exists_

MySQL implementation of WMSpec.Exists
"""
__all__ = []



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