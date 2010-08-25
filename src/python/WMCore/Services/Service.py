#!/usr/bin/env python
"""
_Service_

A Service talks to some http accessible service that provides information. It
has a cache (though this may not be used), an endpoint (the url the service
exists on) a logger and a type (json, xml etc).

Has a default timeout of 30 seconds. Over ride this by passing in a timeout via
the configuration dict, set to None if you want to turn off the timeout.
"""

__revision__ = "$Id: Service.py,v 1.10 2009/06/24 09:21:55 metson Exp $"
__version__ = "$Revision: 1.10 $"

import datetime
import os
import urllib
import time
import socket

class Service:
    def __init__(self, dict={}):
        #The following should read the configuration class
        for a in ['logger', 'endpoint']:
            assert a in dict.keys(), "Can't have a service without a %s" % a

        self.logger = dict['logger']
        self.endpoint = dict['endpoint']

        if 'cachepath' in dict.keys():
            self.path = dict['cachepath']
        else:
            self.path = '/tmp'
        if 'cacheduration' in dict.keys():
            self.cacheduration = dict['cacheduration']
        else:
            self.cacheduration = 0.5
        if 'type' in dict.keys():
            self.type = dict['type']
        else:
            self.type = 'text/xml'

        #Set a timeout for the socket
        self.timeout = 30
        if 'timeout' in dict.keys() and int(dict['timeout']) > timeout:
            self.timeout = int(dict['timeout'])
        elif 'timeout' in dict.keys() and dict['timeout'] == None:
            self.timeout = None

        self.logger.debug("""Service initialised (%s):
\t endpoint: %s (%s)\n\t cache: %s (duration %s hours)""" %
                  (self, self.endpoint, self.type, self.path, self.cacheduration))

    def refreshCache(self, cachefile, url):

        cachefile = "%s/%s" % (self.path, cachefile)

        url = self.endpoint + url

        t = datetime.datetime.now() - datetime.timedelta(hours=self.cacheduration)
        if not os.path.exists(cachefile) or os.path.getmtime(cachefile) < time.mktime(t.timetuple()):
            self.logger.debug("%s expired, refreshing cache" % cachefile)
            getData(cachefile, url)
        return open(cachefile, 'r')

    def forceRefresh(self, cachefile, url):
        cachefile = "%s/%s" % (self.path, cachefile)

        url = self.endpoint + url

        self.logger.debug("Forcing cache refresh of %s" % cachefile)
        getData(cachefile, url)
        return open(cachefile, 'r')

    def clearCache(self, cachefile):
        cachefile = "%s/%s" % (self.path, cachefile)
        try:
            os.remove(cachefile)
        except OSError: # File doesn't exist
            return

    def getData(self, cachefile, url):
        # Set the timeout
        socket.setdefaulttimeout(self.timeout)
        # Get the dat
        u = urllib.URLopener()
        u.addheader('Accept', self.type)
        try:
            u.retrieve(url, cachefile)
        except Exception, e:
            self.logger.exception(e)
            raise e
        # Reset the timeout to 30s
        socket.setdefaulttimeout(None)