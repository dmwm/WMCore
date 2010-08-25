"""
_UpdateWorker_

SQLite implementation of UpdateWorker
"""

__all__ = []
__revision__ = "$Id: UpdateWorker.py,v 1.1 2010/06/21 21:18:40 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Agent.Database.MySQL.UpdateWorker import UpdateWorker \
     as UpdateWorkerMySQL

class UpdateWorker(UpdateWorkerMySQL):
    pass

