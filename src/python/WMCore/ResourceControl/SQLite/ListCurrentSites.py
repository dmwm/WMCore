#!/usr/bin/env python
"""
_ListCurrentSites_

SQLite implementation of ResourceControl.ListCurrentSites
"""




from WMCore.ResourceControl.MySQL.ListCurrentSites \
     import ListCurrentSites as MySQLListCurrentSites

class ListCurrentSites(MySQLListCurrentSites):
    pass