#!/usr/bin/env python
"""
_MarkOpen_

Oracle implementation of Fileset.MarkOpen
"""

__all__ = []



from WMCore.WMBS.MySQL.Fileset.MarkOpen import MarkOpen as MarkOpenFilesetMySQL

class MarkOpen(MarkOpenFilesetMySQL):
    sql = MarkOpenFilesetMySQL.sql
