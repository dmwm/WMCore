#!/usr/bin/env python
"""
_DeleteFileset_

SQLite implementation of Fileset.Delete

"""
__all__ = []
__revision__ = "$Id: Delete.py,v 1.2 2008/11/24 21:51:53 sryu Exp $"
__version__ = "$Revision: 1.2 $"


from WMCore.WMBS.MySQL.Fileset.Delete import Delete as DeleteFilesetMySQL

class Delete(DeleteFilesetMySQL):
    sql = DeleteFilesetMySQL.sql