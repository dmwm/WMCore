#!/usr/bin/env python
"""
_DeleteParentCheck_

Oracle implementation of DeleteParentCheck

"""
__all__ = []



from WMCore.WMBS.MySQL.Files.DeleteParentCheck import DeleteParentCheck as MySQLDeleteParentCheck

class DeleteParentCheck(MySQLDeleteParentCheck):
    """
    Delete parents

    """

    sql = """DELETE FROM wmbs_file_parent WHERE (parent = :id OR child = :id) AND
           NOT EXISTS (SELECT fileset FROM wmbs_fileset_files WHERE fileid = :id
           AND fileset != :fileset)"""
