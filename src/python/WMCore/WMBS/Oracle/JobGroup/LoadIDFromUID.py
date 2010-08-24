#!/usr/bin/env python
"""
_LoadIDFromUID_

Oracle implementation of JobGroup.LoadIDFromUID
"""

__all__ = []
__revision__ = "$Id: LoadIDFromUID.py,v 1.1 2009/01/06 15:54:49 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.JobGroup.LoadIDFromUID import LoadIDFromUID as LoadIDFromUIDMySQL

class LoadIDFromUID(LoadIDFromUIDMySQL):
    sql = "select id from wmbs_jobgroup where guid = :guid"
