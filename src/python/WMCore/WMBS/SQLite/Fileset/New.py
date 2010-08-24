#!/usr/bin/env python
"""
_New_

SQLite implementation of Fileset.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.3 2009/03/03 14:49:10 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Fileset.New import New as NewFilesetMySQL

class New(NewFilesetMySQL):
    sql = NewFilesetMySQL.sql
