#!/usr/bin/env python
"""
_AuthorisedService_

An AuthorisedService is the same as a Service but sends a cert/key with the url
opener to access secured resources.
"""

__revision__ = "$Id: AuthorisedService.py,v 1.11 2009/08/06 17:00:23 metson Exp $"
__version__ = "$Revision: 1.11 $"

import datetime, os, urllib, time

from WMCore.WMException import WMException
from WMCore.Services.Service import Service
from WMCore.Services.Requests import SecureRequests

class AuthorisedService(Service):
    """
    _AuthorisedService_
    
    (Warning: Order of inheritance is important since both has parent have 
     the same method (_getURLOpener))
    TODO: better exception handling - make clear what exception is thrown
    """
    def __init__(self, dict={}):
        try:
            Service.__init__(self, dict)
            self["requests"] = SecureRequests(self["requests"]["host"])
             
        except WMException, ex:
            msg = str(ex)
            self["logger"].exception(msg)
            raise WMException(msg)