#!/usr/bin/env python
"""
_ListByStateAndLocation_

MySQL implementation of ListByStateAndLocation

Created on Feb 21, 2013

@author: dballest
"""

from WMCore.Database.DBFormatter import DBFormatter

class ListByStateAndLocation(DBFormatter):
    """
    DAO to list jobs given certain state and location,
    only returns a list of dictionaries with job information in order
    to match them with BossAir records.
    """
    sql = """SELECT wmbs_job.id, wmbs_job.retry_count,
                    wmbs_job.cache_dir
                FROM wmbs_job
                INNER JOIN wmbs_job_state ON
                    wmbs_job.state = wmbs_job_state.id
                INNER JOIN wmbs_location ON
                    wmbs_location.id = wmbs_job.location
                 WHERE wmbs_job_state.name = :state AND
                     wmbs_location.site_name = :location"""

    def execute(self, state, location, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {'state' : state, 'location' : location},
                                      conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)
