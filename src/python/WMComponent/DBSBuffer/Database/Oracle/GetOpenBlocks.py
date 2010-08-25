#!/usr/bin/env python
"""
_GetOpenBlocks_

Oracle implementation of DBSBuffer.GetOpenBlocks
"""

__revision__ = "$Id: GetOpenBlocks.py,v 1.1 2009/12/07 21:57:29 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.GetOpenBlocks import GetOpenBlocks as MySQLGetOpenBlocks

class GetOpenBlocks(MySQLGetOpenBlocks):
    """
    Identical to MySQL version

    """
