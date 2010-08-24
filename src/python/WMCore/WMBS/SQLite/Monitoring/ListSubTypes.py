#!/usr/bin/env python
"""
_ListSubTypes_

SQLite implementation of Monitoring.ListSubTypes
"""




from WMCore.WMBS.MySQL.Monitoring.ListSubTypes import ListSubTypes \
    as ListSubTypesMySQL

class ListSubTypes(ListSubTypesMySQL):
    pass
