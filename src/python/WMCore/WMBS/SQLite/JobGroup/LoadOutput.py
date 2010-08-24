#!/usr/bin/env python
"""
_LoadOutput_

SQLite implementation of JobGroup.LoadOutput
"""

__all__ = []
__revision__ = "$Id: LoadOutput.py,v 1.1 2008/11/21 17:14:58 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.JobGroup.LoadOutput import LoadOutput as LoadOutputMySQL

class LoadOutput(LoadOutputMySQL):
    sql = LoadOutputMySQL.sql
