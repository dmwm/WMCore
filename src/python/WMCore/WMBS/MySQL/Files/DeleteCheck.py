#!/usr/bin/env python
"""
_DeleteCheckFile_

MySQL implementation of DeleteCheckFile

"""
__all__ = []
__revision__ = "$Id: DeleteCheck.py,v 1.1 2009/09/25 15:14:30 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class DeleteCheck(DBFormatter):
    sql = """DELETE FROM wmbs_file_details WHERE id = :id AND
           NOT EXISTS (SELECT fileset FROM wmbs_fileset_files WHERE file = :id
           AND fileset != :fileset)"""
    
        
    def execute(self, file = None, fileset = None, conn = None, transaction = False):
        binds = {'id': file, 'fileset': fileset}
        self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return True #or raise
