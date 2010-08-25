#!/usr/bin/env python

"""
SQLite implementation of GetChecksum
"""


__revision__ = "$Id: GetChecksum.py,v 1.1 2009/12/02 19:35:36 mnorman Exp $"
__version__  = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Files.GetChecksum import GetChecksum as MySQLGetChecksum

class GetChecksum(MySQLGetChecksum):
    """
    Identical to MySQL

    """
