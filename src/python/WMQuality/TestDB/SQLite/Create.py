#!/usr/bin/env python
"""
_Create_

Install the TestDB schema for SQLite.
"""

__revsion__ = "$Id: Create.py,v 1.1 2010/03/01 16:49:03 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMQuality.TestDB.MySQL.Create import Create as MySQLCreate

class Create(MySQLCreate):
    pass
