#!/usr/bin/env python

"""
Oracle implementation of AddChecksum
"""


__revision__ = "$Id: AddChecksum.py,v 1.1 2009/12/02 19:35:07 mnorman Exp $"
__version__  = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Files.AddChecksum import AddChecksum as MySQLAddChecksum

class AddChecksum(MySQLAddChecksum):
    """
    Identical to MySQL Version

    """
