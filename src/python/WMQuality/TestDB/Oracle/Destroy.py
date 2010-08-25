#!/usr/bin/env python
"""
_Destroy_

Install the TestDB schema for Oracle.
"""

__revsion__ = "$Id: Destroy.py,v 1.1 2010/03/01 16:49:02 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMQuality.TestDB.MySQL.Destroy import Destroy as MySQLDestroy

class Destroy(MySQLDestroy):
    pass
