#!/usr/bin/env python
"""
_List_

MySQL implementation of ListFileset

"""
__all__ = []
__revision__ = "$Id: List.py,v 1.4 2008/11/24 21:47:08 sryu Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    sql = "select id, se_name from wmbs_location order by se_name"
    