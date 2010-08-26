#!/usr/bin/env python
"""
_ListFilesetByTask_

SQLite implementation of Fileset.ListFilesetByTask
"""

__all__ = []
__revision__ = "$Id: ListFilesetByTask.py,v 1.3 2010/06/23 14:40:00 metson Exp $"
__version__ = "$Revision: 1.3 $"


from WMCore.WMBS.MySQL.Fileset.ListFilesetByTask import ListFilesetByTask as ListFilesetByTaskMySQL

class ListFilesetByTask(ListFilesetByTaskMySQL):
    sql = ListFilesetByTaskMySQL.sql