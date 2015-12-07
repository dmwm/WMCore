#!/usr/bin/env python
"""
_List_

MySQL implementation of Locations.List
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    sql = "select id, site_name, pending_slots, running_slots from wmbs_location order by site_name"
