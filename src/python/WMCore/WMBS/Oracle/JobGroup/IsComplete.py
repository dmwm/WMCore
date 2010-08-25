"""
_IsComplete_

Oracle implementation of JobGroup.IsComplete
"""

__all__ = []
__revision__ = "$Id: IsComplete.py,v 1.1 2009/03/24 22:00:14 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.JobGroup.IsComplete import IsComplete as IsCompleteJobGroupMySQL

class IsComplete(IsCompleteJobGroupMySQL):
    sql = IsCompleteJobGroupMySQL.sql