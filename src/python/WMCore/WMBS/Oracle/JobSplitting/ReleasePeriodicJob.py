"""
_ReleasePeriodicJob_

Oracle implementation of JobSplitting.ReleasePeriodicJob
"""

from WMCore.WMBS.MySQL.JobSplitting.ReleasePeriodicJob import ReleasePeriodicJob as MySQLReleasePeriodicJob

class ReleasePeriodicJob(MySQLReleasePeriodicJob):
    """
    Right now this is the same as the MySQL version.

    """
