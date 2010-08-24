#!/usr/bin/env python
"""
_ListWorkloadsForTaskSite_

SQLite implementation of ResourceControl.ListWorkloadsForTaskSite
"""




from WMCore.ResourceControl.MySQL.ListWorkloadsForTaskSite \
     import ListWorkloadsForTaskSite as MySQLListWorkloadsForTaskSite

class ListWorkloadsForTaskSite(MySQLListWorkloadsForTaskSite):
    pass
