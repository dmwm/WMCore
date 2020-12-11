#!/usr/bin/env python
"""
_DeleteParentCheck_

MySQL implementation of DeleteParentCheck

"""
__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class DeleteParentCheck(DBFormatter):
    sql = """DELETE FROM wmbs_file_parent WHERE (parent = :id OR child = :id) AND
           NOT EXISTS (SELECT fileset FROM wmbs_fileset_files WHERE fileid = :id
           AND fileset != :fileset)"""


    def execute(self, file, fileset, conn = None, transaction = False):
        if isinstance(file, list):
            if len(file) < 1:
                # Then we have nothing
                return
            binds = []
            for entry in file:
                binds.append({'id': entry, 'fileset': fileset})
        else:
            binds = {'id': file, 'fileset': fileset}

        self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return True #or raise
