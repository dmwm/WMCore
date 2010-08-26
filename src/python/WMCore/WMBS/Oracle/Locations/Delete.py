#!/usr/bin/env python
"""
_Delete_

Oracle implementation of Locations.Delete
"""

__all__ = []
__revision__ = "$Id: Delete.py,v 1.4 2009/05/09 11:42:27 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.Locations.Delete import Delete as DeleteLocationsMySQL

class Delete(DeleteLocationsMySQL):
    sql = DeleteLocationsMySQL.sql
