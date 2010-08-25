#!/usr/bin/env python
"""
_FailInput_

SQLite implementation of Jobs.FailInput
"""

__revision__ = "$Id: FailInput.py,v 1.1 2009/10/13 20:04:11 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.FailInput import FailInput as MySQLFailInput

class FailInput(MySQLFailInput):
    """
    Identical to MySQL version.
    """
    pass
