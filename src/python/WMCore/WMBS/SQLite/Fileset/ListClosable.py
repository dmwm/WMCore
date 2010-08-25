#!/usr/bin/env python
"""
_ListClosable_

SQLite implementation of Fileset.ListClosable
"""

__revision__ = "$Id: ListClosable.py,v 1.2 2010/04/14 16:01:12 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Fileset.ListClosable import ListClosable as ListFilesetClosableMySQL

class ListClosable(ListFilesetClosableMySQL):
    pass
