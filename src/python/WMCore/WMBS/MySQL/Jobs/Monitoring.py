#!/usr/bin/env python
"""
_Monitoring_

Monitoring DAO classes for Jobs in WMBS
"""
from builtins import map
__all__ = []



from WMCore.Database.DBFormatter import DBFormatter
from functools import reduce

class JobsByState(DBFormatter):
    sql = """select count(wmbs_job.state) as count, wmbs_job_state.name as name from wmbs_job
    join wmbs_job_state on wmbs_job.state=wmbs_job_state.id group by state"""

    def format(self, result):
        list = DBFormatter.formatDict(self, result)
        # [{'count': '2', 'name': 'none'}, {'count': '4', 'name': 'new'}]
        def map_function(item):
            return {item['name'] : item['count']}
        def reduce_function(x, y):
            x.update(y)
            return x
        return reduce(reduce_function, list(map(map_function, list)))
