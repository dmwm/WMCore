#!/usr/bin/env python
"""
_Delete_

MySQL implementation of BossLite.Task.Delete
"""

__all__ = []
__revision__ = "$Id: Delete.py,v 1.2 2010/05/10 12:54:43 spigafi Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class Delete(DBFormatter):
    """
    BossLite.Task.Delete
    """
    
    sql = """DELETE FROM bl_task WHERE %s = :value"""

    def execute(self, value, column = 'id', conn = None, transaction = False):
        """
        This is a generic delete mechanism which allows you to set the column to
        delete by, and pass in a list to value.
        """

        sql = self.sql % (column)

        if type(value) == list:
            binds = value
        else:
            binds = {'value': value}
        
        self.dbi.processData(sql, binds, conn = conn,
                                      transaction = transaction)
        
        # try to catch error code?
        return
