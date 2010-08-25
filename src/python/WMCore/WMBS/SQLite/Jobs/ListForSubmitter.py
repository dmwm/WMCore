#!/usr/bin/env python
"""
_ListForSubmitter_

SQLite implementation of Jobs.ListForSubmitter
"""

__revision__ = "$Id: ListForSubmitter.py,v 1.1 2010/07/28 15:47:01 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.ListForSubmitter import ListForSubmitter as MySQLListForSubmitter

class ListForSubmitter(MySQLListForSubmitter):
    pass
