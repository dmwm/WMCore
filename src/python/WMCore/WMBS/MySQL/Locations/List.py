#!/usr/bin/env python
"""
_List_

MySQL implementation of Locations.List

"""

__all__ = []
__revision__ = "$Id: List.py,v 1.5 2008/12/08 10:12:43 sfoulkes Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    sql = "select id, se_name from wmbs_location order by se_name"
    
