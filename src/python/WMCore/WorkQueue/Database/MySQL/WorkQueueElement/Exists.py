"""
_Exists_

MySQL implementation of WMSpec.Exists
"""
__all__ = []
__revision__ = "$Id: Exists.py,v 1.2 2009/08/18 23:18:15 swakef Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class Exists(DBFormatter):
    sql = """SELECT we.id FROM wq_element we
                INNER JOIN wq_wmspec ws ON (we.wmspec_id = ws.id)
                INNER JOIN wq_block bl ON (we.block_id = bl.id)
            WHERE ws.name=:specName AND bl.name = :blockName"""
    
    def format(self, result):
        result = DBFormatter.format(self, result)
        if len(result) == 0:
            return False
        else:
            return int(result[0][0])
            
    def execute(self, specName, blockName, conn = None, transaction = False):
        binds ={"specName": specName, "blockName": blockName}
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)