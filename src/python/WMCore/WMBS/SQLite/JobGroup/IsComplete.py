"""
_IsComplete_

Sqlite implementation of JobGroup.IsComplete
"""

__all__ = []
__revision__ = "$Id: IsComplete.py,v 1.2 2009/03/31 16:25:16 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.JobGroup.IsComplete import IsComplete as IsCompleteJobGroupMySQL

class IsComplete(IsCompleteJobGroupMySQL):
    sql = """select
               (select count(id) from wmbs_job where jobgroup=:jobgroup) as ac,
               (select count(job) from wmbs_group_job_complete where jobgroup=:jobgroup) as cm
          """
