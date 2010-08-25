"""
_InsertWorker_

SQLite implementation of InsertWorker
"""

__all__ = []
__revision__ = "$Id: InsertWorker.py,v 1.1 2010/06/21 21:18:35 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Agent.Database.MySQL.InsertWorker import InsertWorker \
     as InsertWorkerMySQL

class InsertWorker(InsertWorkerMySQL):
    pass
