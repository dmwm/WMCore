#!/usr/bin/env python

"""
Oracle implementation of AddChecksumByLFN
"""


__revision__ = "$Id: AddChecksumByLFN.py,v 1.1 2010/03/09 20:00:58 mnorman Exp $"
__version__  = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Files.AddChecksumByLFN import AddChecksumByLFN as MySQLAddChecksumByLFN

class AddChecksumByLFN(MySQLAddChecksumByLFN):
    """
    Add Checksums using LFN as file identifier

    """
