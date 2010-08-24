#!/usr/bin/env python
"""
_List_

MySQL implementation of Locations.List

"""

__all__ = []
__revision__ = "$Id: List.py,v 1.6 2009/01/12 19:26:04 sfoulkes Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    sql = "select id, se_name from wmbs_location order by se_name"
