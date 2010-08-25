#!/usr/bin/env python
"""
MySQL implementation of File.SetParentage

Make the parentage link between two file lfns in bulk
"""
__all__ = []
__revision__ = "$Id: SetParentage.py,v 1.1 2010/08/13 16:41:56 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class SetParentage(DBFormatter):
    sql = """INSERT INTO wmbs_file_parent (child, parent)
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
