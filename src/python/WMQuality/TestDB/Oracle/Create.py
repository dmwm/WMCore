#!/usr/bin/env python
"""
_Create_

Install the TestDB schema for Oracle.
"""

__revsion__ = "$Id: Create.py,v 1.2 2010/08/18 15:40:22 meloam Exp $"


from WMQuality.TestDB.MySQL.Create import Create as MySQLCreate

class Create(MySQLCreate):
    pass
