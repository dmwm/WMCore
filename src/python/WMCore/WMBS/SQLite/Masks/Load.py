#!/usr/bin/env python
"""
_Load_

SQLite implementation of Masks.Load
"""

__all__ = []
__revision__ = "$Id: Load.py,v 1.1 2008/11/21 17:13:35 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Masks.Load import Load as LoadMySQL

class Load(LoadMySQL):
    sql = LoadMySQL.sql
