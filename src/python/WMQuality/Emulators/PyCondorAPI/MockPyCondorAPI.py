from __future__ import (division, print_function)


class MockPyCondorAPI(object):
    """
    Version of Services/PyCondor intended to be used with mock or unittest.mock
    """

    def __init__(self, *args, **kwargs):
        print("Using MockPyCondorAPI")

    def getCondorJobs(self, constraint, attr_list):
        """
        Given a job/schedd constraint, return a list of jobs attributes
        or None if the query to condor fails.
        """
        return None

    def isScheddOverloaded(self):
        """check whether job limit is reached in local schedd"""
        return False
