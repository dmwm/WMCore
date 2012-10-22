"""
_UpdateWorker_

Oracle implementation of UpdateWorker
"""

__all__ = []



from WMCore.Agent.Database.MySQL.UpdateWorker import UpdateWorker \
     as UpdateWorkerMySQL

class UpdateWorker(UpdateWorkerMySQL):
    pass
