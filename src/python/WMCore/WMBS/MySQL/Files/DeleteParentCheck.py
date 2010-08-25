#!/usr/bin/env python
"""
_DeleteParentCheck_

MySQL implementation of DeleteParentCheck

"""
__all__ = []
__revision__ = "$Id: DeleteParentCheck.py,v 1.1 2010/04/07 20:29:30 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class DeleteParentCheck(DBFormatter):
    sql = """DELETE FROM wmbs_file_parent WHERE (parent = :id OR child = :id) AND
           NOT EXISTS (SELECT fileset FROM wmbs_fileset_files WHERE file = :id
           AND fileset != :fileset)"""
    
        
    def execute(self, file = None, fileset = None, conn = None, transaction = False):
        binds = {'id': file, 'fileset': fileset}
        self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return True #or raise
