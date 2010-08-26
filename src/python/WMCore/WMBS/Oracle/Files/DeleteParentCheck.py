#!/usr/bin/env python
"""
_DeleteParentCheck_

Oracle implementation of DeleteParentCheck

"""
__all__ = []
__revision__ = "$Id: DeleteParentCheck.py,v 1.1 2010/04/07 20:29:30 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Files.DeleteParentCheck import DeleteParentCheck as MySQLDeleteParentCheck

class DeleteParentCheck(MySQLDeleteParentCheck):
    """
    Delete parents

    """

    sql = """DELETE FROM wmbs_file_parent WHERE (parent = :id OR child = :id) AND
           NOT EXISTS (SELECT fileset FROM wmbs_fileset_files WHERE fileid = :id
           AND fileset != :fileset)"""
