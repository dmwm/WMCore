#!/usr/bin/env python
"""
_Delete_

SQLite implementation of Locations.Delete

"""
__all__ = []
__revision__ = "$Id: Delete.py,v 1.1 2008/10/08 14:30:10 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Locations.Delete import Delete as DeleteLocationsMySQL

class Delete(DeleteLocationsMySQL, SQLiteBase):
    sql = DeleteLocationsMySQL.sql