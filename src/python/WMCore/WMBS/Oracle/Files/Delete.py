#!/usr/bin/env python
"""
_DeleteFile_

Oracle implementation of File.Delete

"""
__all__ = []



from WMCore.WMBS.MySQL.Files.Delete import Delete as DeleteFileMySQL

class Delete(DeleteFileMySQL):
    sql = DeleteFileMySQL.sql
