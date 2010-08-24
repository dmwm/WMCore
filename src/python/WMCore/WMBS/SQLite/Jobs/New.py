#!/usr/bin/env python
"""
_New_
SQLite implementation of Jobs.New
"""
__all__ = []
__revision__ = "$Id: New.py,v 1.4 2009/01/12 19:26:06 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.Jobs.New import New as NewMySQL

class New(NewMySQL):
    sql = NewMySQL.sql
