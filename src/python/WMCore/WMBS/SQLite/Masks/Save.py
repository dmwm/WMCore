#!/usr/bin/env python
"""
_Save_

SQLite implementation of Masks.Save
"""

__all__ = []



from WMCore.WMBS.MySQL.Masks.Save import Save as SaveMySQL

class Save(SaveMySQL):
    sqlBeginning = SaveMySQL.sqlBeginning
