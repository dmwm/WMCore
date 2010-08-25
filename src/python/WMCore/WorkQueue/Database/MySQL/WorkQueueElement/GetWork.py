"""
_GetElements_

MySQL implementation of WorkQueueElement.GetElements
"""

__all__ = []
__revision__ = "$Id: GetWork.py,v 1.1 2009/08/18 23:18:15 swakef Exp $"
__version__ = "$Revision: 1.1 $"

import random
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database import States

#TODO: Investigate a move to one sql call (stored procedure) - should be faster
#TODO: Move end processing logic into SQL query

class GetWork(DBFormatter):
    # get elements that match each site resource ordered by priority
    # elements which do not process any data have their block_id set to NULL
    sql = """SELECT we.subscription_id, wsite.name site_name,
                    we.num_jobs, wbmap.block_id, we.parent_flag
            FROM wq_element we
            LEFT OUTER JOIN wq_block_site_assoc wbmap ON
                                            wbmap.block_id = we.block_id
            LEFT OUTER JOIN wq_site wsite ON (wbmap.site_id = wsite.id)
            WHERE we.status = :available AND we.num_jobs <= :jobs AND (wsite.name = :site OR wsite.name IS NULL)
            ORDER BY (we.priority +
                    :weight * (NOW() - we.insert_time)) DESC
            """

    def execute(self, resources, weight, conn = None, transaction = False):
        binds = [{'available' : States['Available'], 'weight' : weight,
                  "site" : site, "jobs" : jobs} \
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
            site = result['site_name'] or random.choice(resources.keys())
            if  result['subscription_id'] not in acquired_ids and \
                                        result['num_jobs'] <= resources[site]:
                acquired.append(result)
                acquired_ids.append(result['subscription_id'])
                resources[site] = resources[site] - result['num_jobs']
        return acquired
