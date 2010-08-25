#!/usr/bin/env python
"""
_ListClosable_

SQLite implementation of Fileset.ListClosable
"""

__all__ = []
__revision__ = "$Id: ListClosable.py,v 1.1 2009/04/28 13:59:17 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Fileset.ListClosable import ListClosable as ListFilesetClosableMySQL

class ListClosable(ListFilesetClosableMySQL):
    sql = ListFilesetClosableMySQL.sql
