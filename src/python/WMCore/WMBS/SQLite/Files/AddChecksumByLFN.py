#!/usr/bin/env python

"""
SQLite implementation of AddChecksumByLFN
"""


__revision__ = "$Id: AddChecksumByLFN.py,v 1.1 2010/03/09 20:00:59 mnorman Exp $"
__version__  = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Files.AddChecksumByLFN import AddChecksumByLFN as MySQLAddChecksumByLFN

class AddChecksumByLFN(MySQLAddChecksumByLFN):
    """
    Identical to MySQL version

    """
