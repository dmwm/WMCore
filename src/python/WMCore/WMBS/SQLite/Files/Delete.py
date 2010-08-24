#!/usr/bin/env python
"""
_DeleteFile_

SQLite implementation of File.Delete

"""
__all__ = []
__revision__ = "$Id: Delete.py,v 1.2 2008/06/16 16:05:27 metson Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.File.Delete import Delete as DeleteFileMySQL

class Delete(DeleteFileMySQL, SQLiteBase):
    sql = DeleteFileMySQL.sql