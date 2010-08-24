#!/usr/bin/env python
"""
_DeleteFile_

SQLite implementation of File.Delete

"""
__all__ = []
__revision__ = "$Id: Delete.py,v 1.1 2008/10/08 14:30:08 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Files.Delete import Delete as DeleteFileMySQL

class Delete(DeleteFileMySQL, SQLiteBase):
    sql = DeleteFileMySQL.sql