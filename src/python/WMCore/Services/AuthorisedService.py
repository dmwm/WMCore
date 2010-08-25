#!/usr/bin/env python
"""
_AuthorisedService_

An AuthorisedService is the same as a Service but sends a cert/key with the url
opener to access secured resources.
"""

__revision__ = "$Id: AuthorisedService.py,v 1.4 2009/07/07 18:53:20 sryu Exp $"
__version__ = "$Revision: 1.4 $"

import datetime, os, urllib, time

from WMCore.WMException import WMException
from WMCore.WMExceptions import WMEXCEPTION
from WMCore.Services.Service import Service

class AuthorisedService(Service):
    def __init__(self, dict={}):
        Service.__init__(self, dict)
        try:
            self.cert = dict['cert']
            self.key = dict['key']
        except:
            self.logger.exception('Service requires a host certificate and key')
            raise WMException('Service requires a host certificate and key', 
                              "WMCORE-11") 
        
    def refreshCache(self, cachefile, url):
        
        cachefile = "%s/%s" % (self.path, cachefile)
        url = self.endpoint + url
        
        t = datetime.datetime.now() - datetime.timedelta(hours=self.cacheduration)
        if not os.path.exists(cachefile) or os.path.getmtime(cachefile) < time.mktime(t.timetuple()):
            self.logger.debug("%s expired, refreshing cache" % cachefile)
            u = urllib.URLopener(cert_file=self.cert, key_file=self.key)
            u.addheader('Accept', self.type)
            u.retrieve(url, cachefile)
        return open(cachefile, 'r')
    
    def forceRefresh(self, cachefile, url):
        
        cachefile = "%s/%s" % (self.path, cachefile)
        url = self.endpoint + url
        
        self.logger.debug("%s expired, refreshing cache" % cachefile)
        u = urllib.URLopener(cert_file=self.cert, key_file=self.key)
        u.addheader('Accept', self.type)
        u.retrieve(url, cachefile)
        return open(cachefile, 'r')        

