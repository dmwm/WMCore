#!/usr/bin/env python
"""
_DeleteFile_

SQLite implementation of File.Delete

"""
__all__ = []
__revision__ = "$Id: Delete.py,v 1.4 2008/11/20 21:54:25 sryu Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.Files.Delete import Delete as DeleteFileMySQL

class Delete(DeleteFileMySQL):
    sql = DeleteFileMySQL.sql