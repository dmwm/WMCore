from __future__ import (division, print_function)
from builtins import object


class MockMemoryCacheStruct(object):
    """
    Version of Services/PhEDEx intended to be used with mock or unittest.mock
    """

    def __init__(self, *args, **kwargs):
        print("Using MockMemoryCache")

    def getData(self, noFail=True):
        return {}
