#!/usr/bin/env python
"""
_Delete_

Oracle implementation of Locations.Delete

"""
__all__ = []
__revision__ = "$Id: Delete.py,v 1.3 2008/12/05 21:06:24 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Locations.Delete import Delete as DeleteLocationsMySQL

class Delete(DeleteLocationsMySQL):
    sql = DeleteLocationsMySQL.sql