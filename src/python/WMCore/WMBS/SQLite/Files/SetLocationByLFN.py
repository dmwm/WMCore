#!/usr/bin/env python
"""
_SetLocationByLFN_

SQLite implementation of Files.SetLocationByLFN
"""

__revision__ = "$Id: SetLocationByLFN.py,v 1.2 2010/04/08 16:20:09 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Files.SetLocationByLFN import SetLocationByLFN as MySQLSetLocationByLFN

class SetLocationByLFN(MySQLSetLocationByLFN):
    pass
