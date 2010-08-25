#!/usr/bin/env python
"""
_SSLService_

An SSLService is the same as a Service but uses HTTPS to connect to the remote
resources.
"""




import datetime, os, urllib, time

from WMCore.WMException import WMException
from WMCore.Services.Service import Service
from WMCore.Services.Requests import SSLRequests

class SSLService(Service):
    """
    _SSLService_
    
    TODO: better exception handling - make clear what exception is thrown
    """
    def __init__(self, dict={}):
        try:
            # tell Service to use SSLRequests
            self["requests"] = SSLRequests
            Service.__init__(self, dict)

        except WMException, ex:
            msg = str(ex)
            self["logger"].exception(msg)
            raise WMException(msg)