#!/usr/bin/env python
"""
_DeleteCheckFiles_

SQLite implementation of DeleteCheckFiles

"""
__all__ = []



from WMCore.WMBS.MySQL.Files.DeleteCheck import DeleteCheck as MySQLDeleteCheck

class DeleteCheck(MySQLDeleteCheck):
    sql = """DELETE FROM wmbs_file_details WHERE id = :id AND
          NOT EXISTS (SELECT fileset FROM wmbs_fileset_files WHERE fileid = :id
          AND fileset != :fileset)"""

