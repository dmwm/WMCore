#!/usr/bin/env python
"""
_Delete_

MySQL implementation of BossLite.Task.Delete
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class Delete(DBFormatter):
    """
    BossLite.Task.Delete
    """
    
    sql = """DELETE FROM bl_task WHERE %s """
    
    def execute(self, binds, conn = None, transaction = False):
        """
        This is a generic delete mechanism which allows you to 
        set the columns to delete by
        """

        whereStatement = []
        
        for x in binds:
            if type(binds[x]) == str :
                whereStatement.append( "%s = '%s'" % (x, binds[x]) )
            else:
                whereStatement.append( "%s = %s" % (x, binds[x]) )
                
        whereClause = ' AND '.join(whereStatement)

        sqlFilled = self.sql % (whereClause)
        
        self.dbi.processData(sqlFilled, {}, conn = conn,
                                      transaction = transaction)
        
        # try to catch error code?
        return
