#!/usr/bin/env python
"""
_Delete_

SQLite implementation of Locations.Delete

"""
__all__ = []
__revision__ = "$Id: DeleteSQL.py,v 1.1 2008/06/10 11:55:59 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Locations.DeleteSQL import Delete as DeleteLocationsMySQL

class Delete(DeleteLocationsMySQL, SQLiteBase):
    sql = DeleteLocationsMySQL.sql