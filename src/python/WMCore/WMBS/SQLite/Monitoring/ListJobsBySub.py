#!/usr/bin/env python
"""
_ListJobsBySub_

SQLite implementation of Monitoring.ListJobsBySub
"""

__revision__ = "$Id: ListJobsBySub.py,v 1.1 2009/11/17 20:56:01 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Monitoring.ListJobsBySub import ListJobsBySub \
 as ListJobsBySubMySQL

class ListJobsBySub(ListJobsBySubMySQL):
    pass
