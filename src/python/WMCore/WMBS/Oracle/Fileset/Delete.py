#!/usr/bin/env python
"""
_DeleteFileset_

Oracle implementation of Fileset.Delete

"""
__all__ = []




from WMCore.WMBS.MySQL.Fileset.Delete import Delete as DeleteFilesetMySQL

class Delete(DeleteFilesetMySQL):
    sql = DeleteFilesetMySQL.sql
