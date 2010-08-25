#!/usr/bin/env python
"""
_GetBulkRunLumi_

SQLite implementation of GetBulkRunLumi
"""

__revision__ = "$Id: GetBulkRunLumi.py,v 1.1 2010/08/05 16:29:32 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Files.GetBulkRunLumi import GetBulkRunLumi as MySQLGetBulkRunLumi

class GetBulkRunLumi(MySQLGetBulkRunLumi):
    """
    SQLite implementation


    """
