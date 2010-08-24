#!/usr/bin/env python
"""
_DeleteFileset_

SQLite implementation of Fileset.Delete

"""
__all__ = []
__revision__ = "$Id: Delete.py,v 1.1 2008/06/16 16:03:54 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Fileset.Delete import Delete as DeleteFilesetMySQL

class Delete(DeleteFilesetMySQL, SQLiteBase):
    sql = DeleteFilesetMySQL.sql