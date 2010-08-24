#!/usr/bin/env python
"""
_SchedulerCondorG_
"""




import os
from WMCore.BossLite.Scheduler.SchedulerCondorCommon import SchedulerCondorCommon

class SchedulerCondorG(SchedulerCondorCommon) :
    """
    basic class to handle glite jobs through wmproxy API
    """

    def __init__( self, **args ):
        # call super class init method
        super(SchedulerCondorG, self).__init__(**args)


    def specificBulkJdl(self, job, requirements=''):
        # FIXME: This is very similar to SchedulerCondorCommon's version,
        # should be consolidated.
        """
        build a job jdl
        """
        rootName = os.path.splitext(job['standardError'])[0]

        jdl  = 'Universe   = grid\n'
        jdl += 'log     = %s.log\n' % rootName # Same root as stderr
        if self.userRequirements:
            jdl += 'requirements = %s\n' % self.userRequirements

        return jdl
