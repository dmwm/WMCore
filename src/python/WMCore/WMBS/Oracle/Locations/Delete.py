#!/usr/bin/env python
"""
_Delete_

SQLite implementation of Locations.Delete

"""
__all__ = []
__revision__ = "$Id: Delete.py,v 1.2 2008/11/24 21:51:57 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Locations.Delete import Delete as DeleteLocationsMySQL

class Delete(DeleteLocationsMySQL):
    sql = DeleteLocationsMySQL.sql