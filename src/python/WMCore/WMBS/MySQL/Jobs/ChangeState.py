#!/usr/bin/env python
"""
_ChangeState_

MySQL implementation of Jobs.ChangeState.
Expects a list of job objects with, at least, an id, a state (checked by the
wrapper class), a retry count for that state, and an id for the couchdb record
(also added in by the wrapper class, if not present).
"""

__all__ = []
__revision__ = "$Id: ChangeState.py,v 1.5 2009/07/16 22:02:59 meloam Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter

class ChangeState(DBFormatter):
    sql = """UPDATE wmbs_job
            SET state = (select id from wmbs_job_state where name = :state),
                retry_count = :retry,
                couch_record = :couch_record,
                state_time = :time 
            WHERE wmbs_job.id = :job      
                AND wmbs_job.state = (select id from wmbs_job_state where name = :oldstate)
            """

    def getBinds(self, jobs = []):
        """
        pull out state, couch_record, retry_count and id and apply a timestamp
        via a map function. Return a list of dicts for binds.
        """
        def function(job):
            dict = {'job': job['id'],
                    'state': job['state'],
                    'oldstate': job['oldstate'],
                    'time': self.timestamp(),
                    'retry': job['retry_count'],
                    'couch_record': job['couch_record']}
            return dict
        return map(function, jobs)

    def execute(self, jobs = [], conn = None, transaction = False):
        """
        jobs is a list of Job objects (dicts)
        """
        binds = self.getBinds(jobs)
        result =  self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        numberOfResults = result[0].rowcount
        
        if (numberOfResults != len(jobs)):
            raise RuntimeError,\
         "Integrity failure: we tried to update %i records and only %i worked"\
            % (len(jobs),numberOfResults)


        return
