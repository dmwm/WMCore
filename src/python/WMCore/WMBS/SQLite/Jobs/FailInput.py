#!/usr/bin/env python
"""
_FailInput_

SQLite implementation of Jobs.FailInput
"""

__revision__ = "$Id: FailInput.py,v 1.2 2010/04/28 20:43:26 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Jobs.FailInput import FailInput as MySQLFailInput

class FailInput(MySQLFailInput):
    pass
