#!/usr/bin/env python
"""
_New_

SQLite implementation of Masks.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.1 2008/11/21 17:13:35 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Masks.New import New as NewMySQL

class New(NewMySQL):
    sql = NewMySQL.sql
