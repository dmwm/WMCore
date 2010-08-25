#!/usr/bin/env python

"""
Oracle implementation of AddChecksumByLFN
"""


__revision__ = "$Id: AddChecksumByLFN.py,v 1.1 2010/03/09 18:33:43 mnorman Exp $"
__version__  = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.AddChecksumByLFN import AddChecksumByLFN as MySQLAddChecksumByLFN

class AddChecksumByLFN(MySQLAddChecksumByLFN):
    """

    Add Checksums using lfn as key

    """
