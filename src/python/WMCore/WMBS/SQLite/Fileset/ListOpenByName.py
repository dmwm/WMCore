#!/usr/bin/env python
"""
_ListOpenByName_

SQLite implementation of Fileset.ListOpenByName
"""

__all__ = []
__revision__ = "$Id: ListOpenByName.py,v 1.1 2009/11/13 15:08:57 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Fileset.ListOpenByName import ListOpenByName as ListOpenByNameMySQL

class ListOpenByName(ListOpenByNameMySQL):
    sql = ListOpenByNameMySQL.sql
