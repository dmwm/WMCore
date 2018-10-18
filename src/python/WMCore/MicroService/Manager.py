"""
File       : Manager.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: MicroServiceManager class provides full functionality of the MicroService service.
"""

# futures
from __future__ import print_function, division


class MicroServiceManager(object):
    """
    Initialize MicroService  server configuration.
    """
    def __init__(self, config=None):
        self.config = config

    def status(self):
        "Return current status about MicroService"
        sdict = {}
        return sdict

    def request(self, **kwargs):
        "Process request given to MicroService"
        rdict = {}
        if kwargs:
            rdict.update({'input': kwargs})
        return rdict
