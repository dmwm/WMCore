#!/usr/bin/env python
"""
_DeleteFileset_

SQLite implementation of Fileset.Delete

"""
__all__ = []
__revision__ = "$Id: DeleteSQL.py,v 1.1 2008/06/09 16:30:09 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Fileset.DeleteSQL import Delete as DeleteFilesetMySQL

class Delete(DeleteFilesetMySQL, SQLiteBase):
    sql = DeleteFilesetMySQL.sql