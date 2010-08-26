#!/usr/bin/env python
"""
_Save_

SQLite implementation of Masks.Save
"""

__all__ = []
__revision__ = "$Id: Save.py,v 1.2 2009/01/11 17:40:00 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Masks.Save import Save as SaveMySQL

class Save(SaveMySQL):
    sqlBeginning = SaveMySQL.sqlBeginning
