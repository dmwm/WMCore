#!/usr/bin/env python
"""
_ListOpenByName_

Oracle implementation of Fileset.ListOpenByName
"""

__revision__ = "$Id: ListOpenByName.py,v 1.1 2009/11/13 15:11:24 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Fileset.ListOpenByName import ListOpenByName as ListOpenFilesetMySQL

class ListOpenByName(ListOpenFilesetMySQL):
    sql = ListOpenFilesetMySQL.sql
