#!/usr/bin/env python
"""
_Service_

A Service talks to some http(s) accessible service that provides information and
caches the result of these queries. The cache will be refreshed if the file is 
older than a timeout set in the instance of Service. 

It has a cache path (defaults to /tmp), cache duration, an endpoint (the url the 
service exists on) a logger and an accept type (json, xml etc) and method 
(GET/POST). 

The Service satisfies two caching cases:

1. set a defined query, cache results, poll for new ones
2. use a changing query, cache results to a file depending on the query, poll
   for new ones

Data maybe passed to the remote service either via adding the query string to 
the URL (for GET's) or by passing a dictionary to either the service constructor
(case 1.) or by passing the data as a dictionary to the refreshCache, 
forceCache, clearCache calls. By default the cache lasts 30 minutes.

Calling refreshCache/forceRefresh will return an open file object, the cache
file. Once done with it you should close the object.

The service has a default timeout to receive a response from the remote service 
of 30 seconds. Over ride this by passing in a timeout via the configuration 
dict, set to None if you want to turn off the timeout.

If you just want to retrieve the data without caching use the Requests class
directly.

TODO: support etags, respect server expires (e.g. update self['cacheduration'] 
to the expires set on the server if server expires > self['cacheduration'])   
"""

__revision__ = "$Id: Service.py,v 1.30 2010/01/08 23:24:51 metson Exp $"
__version__ = "$Revision: 1.30 $"

SECURE_SERVICES = ('https',)

import datetime
import os
import urllib
from urlparse import urlparse
import time
import socket
from WMCore.Services.Requests import Requests
from WMCore.WMException import WMException

class Service(dict):
    def __init__(self, dict = {}):
        #The following should read the configuration class
        for a in ['logger', 'endpoint']:
            assert a in dict.keys(), "Can't have a service without a %s" % a

        # then split the endpoint into netloc and basepath
        endpoint = urlparse(dict['endpoint'])
        try:
            #Only works on python 2.5 or above
            scheme = endpoint.scheme
            netloc = endpoint.netloc
            path = endpoint.path
        except AttributeError:
            scheme, netloc, path = endpoint[:3]

        # Is this a secure service - add other schemes as we need them
        if dict.get('secure') or scheme in SECURE_SERVICES:
            # only bring in ssl stuff if we really need it
            from WMCore.Services.Requests import SecureRequests
            requests = SecureRequests
        else:
            requests = Requests

        try:
            self.setdefault("basepath", path)
            # Instantiate a Request
            self.setdefault("requests", requests(netloc))
        except WMException, ex:
            msg = str(ex)
            self["logger"].exception(msg)
            raise WMException(msg)

        #set up defaults
        self.setdefault("inputdata", {})
        self.setdefault("cachepath", '/tmp')
        self.setdefault("cacheduration", 0.5)
        self.setdefault("accept_type", 'text/xml')
        self.setdefault("method", 'GET')

        #Set a timeout for the socket
        self.setdefault("timeout", 30)

        # then update with the incoming dict
        self.update(dict)
        self["requests"]["accept_type"] = self["accept_type"]

        self['logger'].debug("""Service initialised (%s):
\t host: %s, basepath: %s (%s)\n\t cache: %s (duration %s hours)""" %
                  (self, self["requests"]["host"], self["basepath"],
                   self["accept_type"], self["cachepath"],
                   self["cacheduration"]))

    def cacheFileName(self, cachefile, inputdata = {}):
        """
        Calculate the cache filename for a given query.
        """
        hash = 0
        if inputdata:
            for key, value in inputdata.items():
                if type(value) == list:
                    value = tuple(value)
                hash += key.__hash__() + value.__hash__()
        else:
            for key, value in self['inputdata'].items():
                if type(value) == list:
                    value = tuple(value)
                hash += key.__hash__() + value.__hash__()
        cachefile = "%s/%s_%s" % (self["cachepath"], hash, cachefile)

        return cachefile

    def refreshCache(self, cachefile, url='', inputdata = {}):
        """
        See if the cache has expired. If it has make a new request to the 
        service for the input data. Return the cachefile as an open file object.  
        """
        t = datetime.datetime.now() - datetime.timedelta(hours = self['cacheduration'])
        cachefile = self.cacheFileName(cachefile, inputdata)

        if not os.path.exists(cachefile) or os.path.getmtime(cachefile) < time.mktime(t.timetuple()):
            self['logger'].debug("%s expired, refreshing cache" % cachefile)
            self.getData(cachefile, url, inputdata)
        return open(cachefile, 'r')

    def forceRefresh(self, cachefile, url='', inputdata = {}):
        """
        Make a new request to the service for the input data, regardless of the 
        cache statue. Return the cachefile as an open file object.  
        """
        cachefile = self.cacheFileName(cachefile, inputdata)

        self['logger'].debug("Forcing cache refresh of %s" % cachefile)
        self.getData(cachefile, url, inputdata)
        return open(cachefile, 'r')

    def clearCache(self, cachefile, inputdata = {}):
        """
        Delete the cache file.
        """
        cachefile = self.cacheFileName(cachefile, inputdata)
        try:
            os.remove(cachefile)
        except OSError: # File doesn't exist
            return

    def getData(self, cachefile, url, inputdata = {}):
        """
        Takes the already generated *full* path to cachefile and the url of the 
        resource. Don't need to call self.cacheFileName(cachefile, inputdata)
        here.
        """
        # Set the timeout
        deftimeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(self['timeout'])

        try:
            # Get the data
            if not inputdata:
                inputdata = self["inputdata"]
            #prepend the basepath
            url = self["basepath"] + str(url)
            self['logger'].debug('getData: \n\turl: %s\n\tdata: %s' % \
                                 (url, inputdata))
            data, status, reason = self["requests"].makeRequest(uri = url,
                                                    verb = self["method"],
                                                    data = inputdata)
            # Don't need to prepend the cachepath, the methods calling getData
            # have done that for us 
            f = open(cachefile, 'w')
            f.write(str(data))
            f.close()
        except Exception, e:
            self['logger'].exception(e)
            raise e
        # Reset the timeout to it's original value
        socket.setdefaulttimeout(deftimeout)
