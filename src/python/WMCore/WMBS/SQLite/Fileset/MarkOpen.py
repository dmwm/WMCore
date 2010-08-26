#!/usr/bin/env python
"""
_MarkOpen_

SQLite implementation of Fileset.MarkOpen
"""

__all__ = []
__revision__ = "$Id: MarkOpen.py,v 1.1 2009/03/03 17:34:19 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Fileset.MarkOpen import MarkOpen as MarkOpenFilesetMySQL

class MarkOpen(MarkOpenFilesetMySQL):
    sql = MarkOpenFilesetMySQL.sql
