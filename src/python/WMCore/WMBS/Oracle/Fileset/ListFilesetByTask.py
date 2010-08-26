#!/usr/bin/env python
"""
_ListFilesetByTask_

Oracle implementation of Fileset.ListFilesetByTask
"""

__revision__ = "$Id: ListFilesetByTask.py,v 1.3 2010/06/24 16:33:04 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"


from WMCore.WMBS.MySQL.Fileset.ListFilesetByTask import ListFilesetByTask as ListFilesetByTaskMySQL

class ListFilesetByTask(ListFilesetByTaskMySQL):
    pass



