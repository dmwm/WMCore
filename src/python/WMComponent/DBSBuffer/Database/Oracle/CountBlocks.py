#!/usr/bin/env python
"""
_CountBlocks_

Oracle implementation of DBSBuffer.CountBlocks
"""

__revision__ = "$Id: CountBlocks.py,v 1.1 2009/09/28 15:12:13 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.CountBlocks import CountBlocks as MySQLCountBlocks

class CountBlocks(MySQLCountBlocks):
    """
    _CountBlocks_

    """
    pass
