from __future__ import (division, print_function)
from builtins import object


class MockPyCondorAPI(object):
    """
    Version of Services/PyCondor intended to be used with mock or unittest.mock
    """

    def __init__(self, *args, **kwargs):
        print("Using MockPyCondorAPI")

    def getCondorJobsSummary(self):
        """
        Mock a condor query for the job summary
        """
        return []

    def getCondorJobs(self, constraint='true', attrList=None, limit=-1, opts="SummaryOnly"):
        """
        Given a job/schedd constraint, return a list of jobs attributes
        or None if the query to condor fails.
        """
        return None

    def isScheddOverloaded(self):
        """check whether job limit is reached in local schedd"""
        return False
