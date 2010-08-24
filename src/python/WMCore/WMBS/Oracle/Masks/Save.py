#!/usr/bin/env python
"""
_Save_

Oracle implementation of Masks.Save
"""

__all__ = []



from WMCore.WMBS.MySQL.Masks.Save import Save as SaveMasksMySQL

class Save(SaveMasksMySQL):
    sqlBeginning = SaveMasksMySQL.sqlBeginning
