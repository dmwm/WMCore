#!/usr/bin/env python
"""
_ListSubTypes_

SQLite implementation of Monitoring.ListSubTypes
"""

__revision__ = "$Id: ListSubTypes.py,v 1.1 2009/11/17 18:50:45 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Monitoring.ListSubTypes import ListSubTypes \
    as ListSubTypesMySQL

class ListSubTypes(ListSubTypesMySQL):
    pass
