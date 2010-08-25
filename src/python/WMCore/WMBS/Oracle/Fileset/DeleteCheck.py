#!/usr/bin/env python
"""
_DeleteCheckFileset_

SQLite implementation of DeleteCheckFileset

"""
__all__ = []



from WMCore.WMBS.MySQL.Fileset.DeleteCheck import DeleteCheck as MySQLDeleteCheck

class DeleteCheck(MySQLDeleteCheck):
    """
    Same as MySQL version

    """
