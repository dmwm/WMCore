#!/usr/bin/env python
"""
_AddBulkParentage_

SQLite implementation of Files.AddBulkParentage
"""

__revision__ = "$Id: AddBulkParentage.py,v 1.1 2009/12/17 22:34:14 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Files.AddBulkParentage import AddBulkParentage as ParentageMySQL

class AddBulkParentage(ParentageMySQL):
    pass
