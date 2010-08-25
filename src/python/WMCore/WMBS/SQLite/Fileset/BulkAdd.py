#!/usr/bin/env python
"""
_BulkAdd_

SQLite implementation of Fileset.BulkAdd
"""

__revision__ = "$Id: BulkAdd.py,v 1.1 2009/10/14 13:39:32 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Fileset.BulkAdd import BulkAdd as BulkAddFilesetMySQL

class BulkAdd(BulkAddFilesetMySQL):
    pass
