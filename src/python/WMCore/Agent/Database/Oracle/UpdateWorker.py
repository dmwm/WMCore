"""
_UpdateWorker_

Oracle implementation of UpdateWorker
"""

__all__ = []
__revision__ = "$Id: UpdateWorker.py,v 1.1 2010/06/21 21:17:55 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Agent.Database.MySQL.UpdateWorker import UpdateWorker \
     as UpdateWorkerMySQL

class UpdateWorker(UpdateWorkerMySQL):
    pass

