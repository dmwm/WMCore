#!/usr/bin/env python
"""
_GetLocationsForJobs_

Oracle implementation of JobGroup.GetLocationsForJobs
"""

__all__ = []
__revision__ = "$Id: GetLocationsForJobs.py,v 1.1 2009/09/15 16:07:01 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import logging
import inspect

from WMCore.WMBS.MySQL.JobGroup.GetLocationsForJobs import GetLocationsForJobs as MySQLGetLocationsForJobs

class GetLocationsForJobs(MySQLGetLocationsForJobs):
    """
    _GetLocationsForJobs_

    Working under the current jobSplitting assumption, namely that each jobGroup contains
    only jobs that can run at the same SET of sites, we can find the jobs the jobGroup can
    run at by selecting them from the files.
    """
    sql = """SELECT DISTINCT site_name FROM wmbs_location wl
          INNER JOIN wmbs_file_location wfl ON wfl.location = wl.id
          INNER JOIN wmbs_job_assoc wja ON wja.fileid = wfl.fileid
          INNER JOIN wmbs_job wj ON wj.id = wja.job
          WHERE wj.jobgroup = :jobgroup"""
