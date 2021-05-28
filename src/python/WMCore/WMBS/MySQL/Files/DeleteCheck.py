#!/usr/bin/env python
"""
_DeleteCheckFile_

MySQL implementation of DeleteCheckFile

"""
__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class DeleteCheck(DBFormatter):
    sql = """DELETE FROM wmbs_file_details WHERE id = :id AND
           NOT EXISTS (SELECT fileset FROM wmbs_fileset_files WHERE fileid = :id
           AND fileset != :fileset)"""


    def execute(self, file = None, fileset = None, conn = None, transaction = False):
        if isinstance(file, list):
            if len(file) < 1:
                return
            binds = []
            for entry in file:
                binds.append({'id': entry, 'fileset': fileset})
        else:
            binds = {'id': file, 'fileset': fileset}
        self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return True #or raise
