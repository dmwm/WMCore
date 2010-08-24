#!/usr/bin/env python
"""
_Save_

MySQL implementation of Masks.Save
"""

__all__ = []
__revision__ = "$Id: Save.py,v 1.1 2008/11/24 21:51:49 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Masks.Save import Save as SaveMasksMySQL

class Save(SaveMasksMySQL):
    def execute(self, jobid, mask):
        SaveMasksMySQL.execute(self, jobid, mask)
