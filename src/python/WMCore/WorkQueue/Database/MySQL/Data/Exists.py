"""
_Exists_

MySQL implementation of WMSpec.Exists
"""
__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class Exists(DBFormatter):
    sql = """SELECT id FROM wq_data WHERE name = :name"""
    
    def format(self, result):
        result = DBFormatter.format(self, result)
        if len(result) == 0:
            return False
        else:
            return int(result[0][0])
        
    def execute(self, name = None, conn = None, transaction = False):
        binds = {'name': name}
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)
