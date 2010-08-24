#!/usr/bin/env python
"""
_DeleteFile_

SQLite implementation of File.Delete

"""
__all__ = []
__revision__ = "$Id: Delete.py,v 1.2 2008/11/24 21:51:33 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Files.Delete import Delete as DeleteFileMySQL

class Delete(DeleteFileMySQL):
    sql = DeleteFileMySQL.sql