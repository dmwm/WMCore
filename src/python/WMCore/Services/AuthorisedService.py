#!/usr/bin/env python
"""
_AuthorisedService_

An AuthorisedService is the same as a Service but sends a cert/key with the url
opener to access secured resources.
"""

__revision__ = "$Id: AuthorisedService.py,v 1.10 2009/07/16 21:57:26 metson Exp $"
__version__ = "$Revision: 1.10 $"

import datetime, os, urllib, time

from WMCore.WMException import WMException
from WMCore.Services.Service import Service
from WMCore.Services.Requests import SecureRequests as Requests

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
        except WMException, ex:
            msg = str(ex)
            self["logger"].exception(msg)
            raise WMException(msg)