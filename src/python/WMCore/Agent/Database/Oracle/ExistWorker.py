"""
_ExistWorker_

Oracle implementation of ExistWorker
"""

__all__ = []



from WMCore.Agent.Database.MySQL.ExistWorker import ExistWorker \
     as ExistWorkerMySQL

class ExistWorker(ExistWorkerMySQL):
    pass
