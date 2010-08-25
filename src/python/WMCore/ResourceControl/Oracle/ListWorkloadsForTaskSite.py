#!/usr/bin/env python
"""
_ListWorkloadsForTaskSite_

Oracle implementation of ResourceControl.ListWorkloadsForTaskSite
"""

__revision__ = "$Id: ListWorkloadsForTaskSite.py,v 1.1 2010/07/07 19:20:55 sfoulkes Exp $"
__version__  = "$Revision: 1.1 $"

from WMCore.ResourceControl.MySQL.ListWorkloadsForTaskSite \
     import ListWorkloadsForTaskSite as MySQLListWorkloadsForTaskSite

class ListWorkloadsForTaskSite(MySQLListWorkloadsForTaskSite):
    pass
