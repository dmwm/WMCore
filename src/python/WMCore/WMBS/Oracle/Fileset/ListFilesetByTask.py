#!/usr/bin/env python
"""
_ListFilesetByTask_

Oracle implementation of Fileset.ListFilesetByTask
"""

__all__ = []
__revision__ = "$Id: ListFilesetByTask.py,v 1.1 2010/06/01 17:14:53 riahi Exp $"
__version__ = "$Revision: 1.1 $"


from WMCore.WMBS.MySQL.Fileset.ListFilesetByTask import ListFilesetByTask as ListFilesetByTaskMySQL

class LoadFromName(ListFilesetByTaskMySQL):
    sql = ListFilesetByTaskMySQL.sql
~                                     


