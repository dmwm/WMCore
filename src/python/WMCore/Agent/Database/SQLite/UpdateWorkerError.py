"""
_UpdateWorkerError_

SQLite implementation of UpdateWorker
"""

__all__ = []
__revision__ = "$Id: UpdateWorkerError.py,v 1.1 2010/06/23 18:07:04 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Agent.Database.MySQL.UpdateWorkerError import UpdateWorkerError \
     as UpdateWorkerErrorMySQL

class UpdateWorkerError(UpdateWorkerErrorMySQL):
    pass

