#!/usr/bin/env python
"""
_ListFilesetByTask_

Oracle implementation of Fileset.ListFilesetByTask
"""

__all__ = []
__revision__ = "$Id: ListFilesetByTask.py,v 1.2 2010/06/01 21:19:41 riahi Exp $"
__version__ = "$Revision: 1.2 $"


from WMCore.WMBS.MySQL.Fileset.ListFilesetByTask import ListFilesetByTask as ListFilesetByTaskMySQL

class ListFilesetByTask(ListFilesetByTaskMySQL):
    sql = ListFilesetByTaskMySQL.sql
~                                     


