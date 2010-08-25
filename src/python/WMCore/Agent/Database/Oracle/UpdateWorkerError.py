"""
_UpdateWorkerError_

Oracle implementation of UpdateWorker
"""

__all__ = []



from WMCore.Agent.Database.MySQL.UpdateWorkerError import UpdateWorkerError \
     as UpdateWorkerErrorMySQL

class UpdateWorkerError(UpdateWorkerErrorMySQL):
    pass
