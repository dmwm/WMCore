#!/usr/bin/env python
"""
_DeleteParentCheck_

MySQL implementation of DeleteParentCheck

"""
__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class DeleteParentCheck(DBFormatter):
    sql = """DELETE FROM wmbs_file_parent WHERE (parent = :id OR child = :id) AND
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
