#!/usr/bin/env python
"""
_Load_

MySQL implementation of Masks.Load
"""

__all__ = []
__revision__ = "$Id: Load.py,v 1.1 2008/11/24 21:51:50 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Masks.Load import Load as LoadMasksMySQL

class Load(LoadMasksMySQL):
    sql = LoadMasksMySQL.sql