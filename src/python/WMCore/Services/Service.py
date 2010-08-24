#!/usr/bin/env python
"""
_Service_

A Service talks to some http accessible service that provides information. It
has a cache (though this may not be used), an endpoint (the url the service
exists on) a logger and a type (json, xml etc).
"""

__revision__ = "$Id: Service.py,v 1.4 2008/11/29 22:30:05 metson Exp $"
__version__ = "$Revision: 1.4 $"

import datetime
import os
import urllib
import time
import socket

class Service:
    def __init__(self, dict={}):
        #The following should (also) read the configuration class
        try:
            self.logger = dict['logger']
            try:
                self.endpoint = dict['endpoint']
                try:
                    self.path = dict['cachepath']
                except:
                    self.path = '/tmp'
                try:
                    self.cacheduration = dict['cacheduration']
                except:
                    self.cacheduration = 0.5
            except:
                self.logger.exception('trying to initialise a Service without'\
                                      ' an endpoint: FAIL')
                raise TypeError, "Can't have a service without an endpoint"
        except:
            raise TypeError, "Can't have a service without a logger"

        #Set a timeout for the socket
        timeout = 30
        if 'timeout' in dict.keys() and int(dict['timeout']) > timeout:
            timeout = int(dict['timeout'])
        socket.setdefaulttimeout(timeout)

        try:
            self.type = dict['type']
        except:
            self.type = 'text/xml'

        self.logger.debug("""Service initialised (%s):
\t endpoint: %s (%s)\n\t cache: %s (duration %s hours)""" %
                  (self, self.endpoint, self.type, self.path, self.cacheduration))

    def refreshCache(self, cachefile, url):

        cachefile = "%s/%s" % (self.path, cachefile)

        url = self.endpoint + url

        t = datetime.datetime.now() - datetime.timedelta(hours=self.cacheduration)
        if not os.path.exists(cachefile) or os.path.getmtime(cachefile) < time.mktime(t.timetuple()):
            self.logger.debug("%s expired, refreshing cache" % cachefile)
            u = urllib.URLopener()
            u.addheader('Accept', self.type)
            try:
                u.retrieve(url, cachefile)
            except Exception, e:
                self.logger.exception(e)
                raise e
        return open(cachefile, 'r')
    
    def forceRefresh(self, cachefile, url):
        cachefile = "%s/%s" % (self.path, cachefile)

        url = self.endpoint + url

        self.logger.debug("Forcing cache refresh of %s" % cachefile)
        u = urllib.URLopener()
        u.addheader('Accept', self.type)
        try:
            u.retrieve(url, cachefile)
        except Exception, e:
            self.logger.exception(e)
            raise e
        return open(cachefile, 'r')