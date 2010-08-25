#!/usr/bin/env python
"""
_Service_

A Service talks to some http accessible service that provides information. It
has a cache (though this may not be used), an endpoint (the url the service
exists on) a logger and a type (json, xml etc).

Has a default timeout of 30 seconds. Over ride this by passing in a timeout via
the configuration dict, set to None if you want to turn off the timeout.

If you just want to retrieve the data without caching use the Requests class
directly.
"""

__revision__ = "$Id: Service.py,v 1.15 2009/07/11 08:48:16 metson Exp $"
__version__ = "$Revision: 1.15 $"

import datetime
import os
import urllib
import time
import socket
from urlparse import urlparse
from WMCore.Services.Requests import Requests

class Service(Requests):
    def __init__(self, dict={}):
        #The following should read the configuration class
        for a in ['logger', 'endpoint']:
            assert a in dict.keys(), "Can't have a service without a %s" % a

        # Inherit from Resource
        # then split the endpoint into netloc and basepath
        endpoint = urlparse(dict['endpoint'])
        self.setdefault("basepath", endpoint.path)
        Requests.__init__(self, endpoint.netloc)
        
         #set up defaults
        self.setdefault("cachepath", '/tmp')
        self.setdefault("cacheduration", 0.5)
        self.setdefault("accept_type", 'text/xml')
        self.setdefault("method", 'GET')
        
        #Set a timeout for the socket
        self.setdefault("timeout", 30)
        
        # then update with the incoming dict
        self.update(dict)
      
        
        self['logger'].debug("""Service initialised (%s):
\t host: %s, basepath: %s (%s)\n\t cache: %s (duration %s hours)""" %
                  (self, self["host"], self["basepath"], self["accept_type"], self["cachepath"], 
                   self["cacheduration"]))

    def refreshCache(self, cachefile, url):
        cachefile = "%s/%s" % (self["cachepath"], cachefile)

        t = datetime.datetime.now() - datetime.timedelta(hours=self['cacheduration'])
        if not os.path.exists(cachefile) or os.path.getmtime(cachefile) < time.mktime(t.timetuple()):
            self['logger'].debug("%s expired, refreshing cache" % cachefile)
            self.getData(cachefile, url)
        return open(cachefile, 'r')

    def forceRefresh(self, cachefile, url):
        cachefile = "%s/%s" % (self["cachepath"], cachefile)

        self['logger'].debug("Forcing cache refresh of %s" % cachefile)
        self.getData(cachefile, url)
        return open(cachefile, 'r')

    def clearCache(self, cachefile):
        cachefile = "%s/%s" % (self["cachepath"], cachefile)
        try:
            os.remove(cachefile)
        except OSError: # File doesn't exist
            return

    def getData(self, cachefile, url):
        """
        Takes the *full* path to the cachefile and the url of the resource.
        """
        # Set the timeout
        deftimeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(self['timeout'])
        try:
            # Get the data
            url = self["basepath"] + url
            data, status, reason = self.makeRequest(uri=url, type=self["method"])
            # Don't need to prepend the cachepath, methods calling getData have
            # done that for us 
            f = open(cachefile, 'w')
            f.write(data)
            f.close()
        except Exception, e:
            self['logger'].exception(e)
            raise e
        # Reset the timeout to None
        socket.setdefaulttimeout(deftimeout)