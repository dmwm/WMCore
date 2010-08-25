#!/usr/bin/env python
"""
_Exists_

SQLite implementation of Files.Exists
"""

__all__ = []
__revision__ = "$Id: Exists.py,v 1.1 2009/05/14 16:21:51 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.Exists import Exists as MySQLExists

class Exists(MySQLExists):
    """
    _Exists_
    
    SQLite implementation of Files.Exists
    """
    sql = "select id from dbsbuffer_file where lfn = :lfn"
    

