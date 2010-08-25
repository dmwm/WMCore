#!/usr/bin/env python
"""
_Exists_

SQLite implementation of Files.Exists
"""

__all__ = []
__revision__ = "$Id: Exists.py,v 1.2 2009/07/13 19:31:54 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.Exists import Exists as MySQLExists

class Exists(MySQLExists):
    """
    _Exists_
    
    SQLite implementation of Files.Exists
    """
    pass
    

