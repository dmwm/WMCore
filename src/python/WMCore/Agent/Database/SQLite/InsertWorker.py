"""
_InsertWorker_

SQLite implementation of InsertWorker
"""

__all__ = []



from WMCore.Agent.Database.MySQL.InsertWorker import InsertWorker \
     as InsertWorkerMySQL

class InsertWorker(InsertWorkerMySQL):
    pass
