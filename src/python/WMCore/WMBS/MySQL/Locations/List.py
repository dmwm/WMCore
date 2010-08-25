#!/usr/bin/env python
"""
_List_

MySQL implementation of Locations.List
"""

__all__ = []
__revision__ = "$Id: List.py,v 1.8 2009/05/09 12:05:35 sfoulkes Exp $"
__version__ = "$Revision: 1.8 $"

from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    sql = "select id, site_name, job_slots from wmbs_location order by site_name"
