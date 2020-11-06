from __future__ import (division, print_function)
from builtins import object

class MockLogDB(object):
    """
    Version of Services/PhEDEx intended to be used with mock or unittest.mock
    """

    def __init__(self, *args, **kwargs):
        print("Using MockLogDB")

    def post(self, request=None, msg="", mtype="comment"):
        """Post new entry into LogDB for given request"""

        return 'OK'

    def get(self, request=None, mtype=None):
        """Retrieve all entries from LogDB for given request"""
        # TODO Need to add mock data
        return []

    def get_all_requests(self):
        """Retrieve all entries from LogDB for given request"""
        # TODO Need to add mock data
        return []

    def delete(self, request=None, mtype=None, this_thread=False):
        """
        Delete entry in LogDB for given request
        if mtype == None - delete all the log for that request
        mtype != None - only delete specified mtype
        """

        return "OK"

    def wmstats_down_components_report(self, thread_list):
        # TODO Need to add mock data
        return {}