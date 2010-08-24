#!/usr/bin/env python
"""
_DeleteCheckFiles_

SQLite implementation of DeleteCheckFiles

"""
__all__ = []



from WMCore.WMBS.MySQL.Files.DeleteCheck import DeleteCheck as MySQLDeleteCheck

class DeleteCheck(MySQLDeleteCheck):
    """
    Same as MySQL version

    """
