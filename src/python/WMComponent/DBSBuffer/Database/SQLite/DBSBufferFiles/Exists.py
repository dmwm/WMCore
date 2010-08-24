#!/usr/bin/env python
"""
_Exists_

SQLite implementation of Files.Exists
"""

__all__ = []



from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.Exists import Exists as MySQLExists

class Exists(MySQLExists):
    """
    _Exists_
    
    SQLite implementation of Files.Exists
    """
    pass
    

