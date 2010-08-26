#!/usr/bin/env python
"""
_DeleteCheckFile_

MySQL implementation of DeleteCheckFile

"""
__all__ = []
__revision__ = "$Id: DeleteCheck.py,v 1.2 2010/05/24 15:38:31 mnorman Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class DeleteCheck(DBFormatter):
    sql = """DELETE FROM wmbs_file_details WHERE id = :id AND
           NOT EXISTS (SELECT fileset FROM wmbs_fileset_files WHERE file = :id
           AND fileset != :fileset)"""
    
        
    def execute(self, file = None, fileset = None, conn = None, transaction = False):
        if type(file) == list:
            binds = []
            for entry in file:
                binds.append({'id': entry, 'fileset': fileset})
        else:
            binds = {'id': file, 'fileset': fileset}
        self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return True #or raise
