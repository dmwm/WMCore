#!/usr/bin/env python
"""
_DeleteFile_

Oracle implementation of File.Delete

"""
__all__ = []
__revision__ = "$Id: Delete.py,v 1.3 2008/12/05 21:06:24 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Files.Delete import Delete as DeleteFileMySQL

class Delete(DeleteFileMySQL):
    sql = DeleteFileMySQL.sql