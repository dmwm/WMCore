"""
_GetElements_

MySQL implementation of WorkQueueElement.GetElements
"""

__all__ = []
__revision__ = "$Id: GetWork.py,v 1.8 2009/11/30 20:13:33 sryu Exp $"
__version__ = "$Revision: 1.8 $"

import random
import time
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database import States

#TODO: Investigate a move to one sql call (stored procedure) - should be faster
#TODO: Move end processing logic into SQL query

class GetWork(DBFormatter):
    # get elements that match each site resource ordered by priority
    # elements which do not process any data have their input_id set to NULL
    sql = """SELECT we.id, we.wmtask_id, we.subscription_id, wsite.name site_name,
                    we.num_jobs, we.input_id, we.parent_flag
            FROM wq_element we
            LEFT JOIN wq_data_site_assoc wbmap ON
                    wbmap.data_id = we.input_id
            LEFT JOIN wq_site wsite ON (wbmap.site_id = wsite.id)
            WHERE we.status = :available AND
                  we.num_jobs <= :jobs AND
                  -- If have input data release to site with that data,
                  -- else can release to any site
                  (wsite.name = :site OR
                          (wsite.name IS NULL AND we.input_id is NULL))
            ORDER BY (we.priority +
                    :weight * (:current_time - we.insert_time)) DESC
            """

    def execute(self, resources, weight, conn = None, transaction = False):
        binds = [{'available' : States['Available'], 'weight' : weight,
                  "site" : site, "jobs" : jobs, "current_time": int(time.time())} \
                                    for site, jobs in resources.iteritems()]
        results = self.dbi.processData(self.sql, binds, conn = conn,
                         transaction = transaction)
        results = self.formatDict(results)
        acquired_ids = []
        acquired = []
        # the sql query will return all matching blocks so loop over them
        # (in priority order) and limit to the job slots at each site.
        # Strip out duplicate elements (which can run at multiple sites.)
        for result in results:
            # Work which requires input data must be assigned to a particular
            # site. If this fails something has gone wrong with the sql call
            assert (result['site_name'] is None) == \
                   (result['input_id'] is None), \
                   'Input data required but not released to a specific site'

            # Production jobs (can run anywhere) are assigned to a random site
            site = result['site_name'] or random.choice(resources.keys())
            if  result['id'] not in acquired_ids and \
                                    result['num_jobs'] <= resources[site]:
                acquired.append(result)
                acquired_ids.append(result['id'])
                resources[site] = int(resources[site]) - result['num_jobs']
        return acquired
