#!/usr/bin/env python
"""
_Exists_

SQLite implementation of Jobs.Exists
"""

__all__ = []
__revision__ = "$Id: Exists.py,v 1.1 2008/11/21 17:14:14 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.Exists import Exists as ExistsMySQL

class Exists(ExistsMySQL):
    sql = ExistsMySQL.sql
