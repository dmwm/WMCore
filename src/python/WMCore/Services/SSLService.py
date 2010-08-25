#!/usr/bin/env python
"""
_SSLService_

An SSLService is the same as a Service but uses HTTPS to connect to the remote
resources.
"""

__revision__ = "$Id: SSLService.py,v 1.1 2009/08/18 14:22:34 metson Exp $"
__version__ = "$Revision: 1.1 $"

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
            Service.__init__(self, dict)
            self["requests"] = SSLRequests(self["requests"]["host"])
             
        except WMException, ex:
            msg = str(ex)
            self["logger"].exception(msg)
            raise WMException(msg)