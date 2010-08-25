"""
_ExistWorker_

SQLite implementation of ExistWorker
"""

__all__ = []
__revision__ = "$Id: ExistWorker.py,v 1.1 2010/06/21 21:18:35 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Agent.Database.MySQL.ExistWorker import ExistWorker \
     as ExistWorkerMySQL

class ExistWorker(ExistWorkerMySQL):
    pass