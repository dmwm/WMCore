#!/usr/bin/env python
"""
_Save_

SQLite implementation of Jobs.Save
"""

__all__ = []
__revision__ = "$Id: Save.py,v 1.1 2008/11/21 17:14:14 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.Save import Save as SaveMySQL

class Save(SaveMySQL):
    sql = """UPDATE wmbs_job SET JOBGROUP = :jobgroup, NAME = :name,
              LAST_UPDATE = strftime('%s', 'now') WHERE ID = :jobid"""    
