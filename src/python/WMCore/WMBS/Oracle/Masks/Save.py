#!/usr/bin/env python
"""
_Save_

Oracle implementation of Masks.Save
"""

__all__ = []
__revision__ = "$Id: Save.py,v 1.3 2009/01/11 17:48:26 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Masks.Save import Save as SaveMasksMySQL

class Save(SaveMasksMySQL):
    sqlBeginning = SaveMasksMySQL.sqlBeginning
