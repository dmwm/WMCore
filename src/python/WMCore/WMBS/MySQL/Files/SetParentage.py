#!/usr/bin/env python
"""
MySQL implementation of File.SetParentage

Make the parentage link between two file lfns in bulk
"""
__all__ = []
__revision__ = "$Id: SetParentage.py,v 1.2 2010/08/13 18:52:50 mnorman Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class SetParentage(DBFormatter):
    sql = """INSERT IGNORE INTO wmbs_file_parent (child, parent)
             SELECT DISTINCT wfd1.id, wfd2.id
             FROM wmbs_file_details wfd1 INNER JOIN wmbs_file_details wfd2
             WHERE wfd1.lfn = :child
             AND wfd2.lfn = :parent
    """
    
    
    def format(self, result):
        return True
    
    def execute(self, binds, conn = None, transaction = False):
        
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)
