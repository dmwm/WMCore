#!/usr/bin/env python
"""
_SetLocationByLFN_

SQLite implementation of Files.SetLocationByLFN
"""

__revision__ = "$Id: SetLocationByLFN.py,v 1.1 2010/03/09 19:59:26 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Files.SetLocationByLFN import SetLocationByLFN as MySQLSetLocationByLFN

class SetLocationByLFN(MySQLSetLocationByLFN):
    """
    Set Locations using LFNs

    """
