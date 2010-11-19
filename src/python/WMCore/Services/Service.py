#!/usr/bin/env python
"""
_Service_

A Service talks to some http(s) accessible service that provides information and
caches the result of these queries. The cache will be refreshed if the file is 
older than a timeout set in the instance of Service. 

It has a cache path, cache duration, an endpoint (the url the 
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

The Service class provides two layers of caching:
    1. Caching from httplib2 is provided via Request, this respectsetag and 
    expires, but the cache will be lost if the service raises an exception or 
    similar.
    2. Internal caching which respects an internal cache duration. If the remote
    service fails to respond the second layer cache will be used until the cache
    dies.

In tabular form:

httplib2 cache  |   yes    |   yes    |    no    |     no     |
----------------+----------+----------+----------+------------+
service cache   |    no    |   yes    |   yes    |     no     |
----------------+----------+----------+----------+------------+
result          |  cached  |  cached  |  cached  | not cached |
"""




SECURE_SERVICES = ('https',)

import datetime
import os
import httplib2
from httplib import InvalidURL
from urlparse import urlparse
import time
import socket
from httplib import HTTPException
from WMCore.Services.Requests import Requests
from WMCore.WMException import WMException
from WMCore.Wrappers import JsonWrapper as json
import types
import logging
import stat
import tempfile
import shutil
from WMCore.Algorithms import Permissions

class Service(dict):
    
    def __init__(self, dict = {}):
        #The following should read the configuration class
        for a in ['endpoint']:
            assert a in dict.keys(), "Can't have a service without a %s" % a

        scheme = ''
        netloc = ''
        path = ''
        
        #if end point ends without '/', add that
        if not dict['endpoint'].endswith('/'):
            dict['endpoint'] = dict['endpoint'].strip() + '/'

        # then split the endpoint into netloc and basepath
        endpoint_components = urlparse(dict['endpoint'])
        
        try:
            #Only works on python 2.5 or above
            scheme = endpoint_components.scheme
            netloc = endpoint_components.netloc
            path = endpoint_components.path
        except AttributeError:
            scheme, netloc, path = endpoint_components[:3]

        #set up defaults
        self.setdefault("inputdata", {})
        self.setdefault("cacheduration", 0.5)
        self.setdefault("maxcachereuse", 24.0)
        self.supportVerbList = ('GET', 'POST', 'PUT', 'DELETE')
        # this value should be only set when whole service class uses
        # the same verb ('GET', 'POST', 'PUT', 'DELETE')
        self.setdefault("method", None)

        # object to store temporary directory - cleaned up on destruction
        self['deleteCacheOnExit'] = None

        #Set a timeout for the socket
        self.setdefault("timeout", 30)

        # then update with the incoming dict
        self.update(dict)

        self['cachepath'] = self.cachePath(self.get('cachepath'), netloc)
        # we want the request object to cache to a known location
        dict['req_cache_path'] = self['cachepath'] + '/requests'

        if 'logger' not in self:
            logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename = os.path.join(self['cachepath'], '%s.log' % self.__class__.__name__.lower()),
                    filemode='w')
            self['logger'] = logging.getLogger(self.__class__.__name__)

        # Get the request class, to instantiate later
        # either passed as param to __init__, determine via scheme or default
        if type(self.get('requests')) == types.TypeType:
            requests = self['requests']
        # Is this a secure service - add other schemes as we need them
        elif self.get('secure', False) or scheme in SECURE_SERVICES:
            # only bring in ssl stuff if we really need it
            from WMCore.Services.Requests import SecureRequests
            requests = SecureRequests
        else:
            requests = Requests
        # Instantiate a Request
        try:
            self["requests"] = requests(dict['endpoint'], dict)
        except WMException, ex:
            msg = str(ex)
            self["logger"].exception(msg)
            raise

        self['logger'].debug("""Service initialised (%s):
\t host: %s, basepath: %s (%s)\n\t cache: %s (duration %s hours, max reuse %s hours)""" %
                  (self, self["requests"]["host"], self["endpoint"],
                   self["requests"]["accept_type"], self["cachepath"],
                   self["cacheduration"], self["maxcachereuse"]))


    def cachePath(self, given_path, netlocoation):
        """Return cache location"""
        top = self.cacheTopPath(given_path)

        # deal with multiple Services that have the same service running and
        # with multiple users for a given Service
        if netlocoation.find("@") == -1:
            cachepath = os.path.join(top, netlocoation)
        else:
            auth, server_url = netlocoation.split('@')
            user = auth.split(':')[0]
            cachepath = os.path.join(top, '%s-%s' % (user, server_url))

        try:
            # only we should be able to write to this dir
            os.makedirs(cachepath, stat.S_IRWXU)
        except OSError:
            if not os.path.isdir(cachepath):
                raise
            Permissions.owner_readwriteexec(cachepath)

        return cachepath


    def cacheTopPath(self, given_path):
        """Where to cache results?

        Logic:
          o If passed in take that
          o Is the environemnt variable "self.__class__".upper()_CACHE defined?
          o Is WMCORE_CACHE_DIR set
          o Generate a temporary directory
          """
        if given_path:
            return given_path
        user = str(os.getuid())
        # append user id so users don't clobber each other
        lastbit = os.path.join('.wmcore_cache_%s' % user, self.__class__.__name__.lower())
        for var in ('%s_CACHE_DIR' % self.__class__.__name__.upper(),
                    'WMCORE_CACHE_DIR'):
            if os.environ.get(var):
                firstbit = os.environ[var]
                break
        else:
            dir = tempfile.mkdtemp(prefix='.wmcore_cache')
            self['deleteCacheOnExit'] = TempDirectory(dir)
            return dir

        return os.path.join(firstbit, lastbit)


    def _makeHash(self, inputdata, hash):
        """
        Turn the input data into json and hash the string. This is simple and 
        means that the input data must be json-serialisable, which is good.
        """
        json_hash = json.dumps(inputdata)
        return json_hash.__hash__()     
        
    def cacheFileName(self, cachefile, verb='GET', inputdata = {}):
        """
        Calculate the cache filename for a given query.
        """
        
        hash = 0
        if inputdata:
            hash = self._makeHash(inputdata, hash)
        else:
            hash = self._makeHash(self['inputdata'], hash)
        cachefile = "%s/%s_%s_%s" % (self["cachepath"], hash, verb, cachefile)

        return cachefile

    def refreshCache(self, cachefile, url='', inputdata = {}, openfile=True, 
                     encoder = True, decoder = True, verb = 'GET', contentType = None):
        """
        See if the cache has expired. If it has make a new request to the 
        service for the input data. Return the cachefile as an open file object.  
        """
        verb = self._verbCheck(verb)
        
        t = datetime.datetime.now() - datetime.timedelta(hours = self['cacheduration'])
        cachefile = self.cacheFileName(cachefile, verb, inputdata)
        
        if not os.path.exists(cachefile) or os.path.getmtime(cachefile) < time.mktime(t.timetuple()):
            self['logger'].debug("%s expired, refreshing cache" % cachefile)
            self.getData(cachefile, url, inputdata, {}, encoder, decoder, verb, contentType)

        if openfile:
            return open(cachefile, 'r')
        else:
            return cachefile

    def forceRefresh(self, cachefile, url='', inputdata = {}, openfile=True,
                     encoder = True, decoder = True, verb = 'GET', contentType = None):
        """
        Make a new request to the service for the input data, regardless of the 
        cache state. Return the cachefile as an open file object.  
        """
        verb = self._verbCheck(verb)
        
        cachefile = self.cacheFileName(cachefile, verb, inputdata)

        self['logger'].debug("Forcing cache refresh of %s" % cachefile)
        self.getData(cachefile, url, inputdata, {'cache-control':'no-cache'}, 
                     encoder, decoder, verb, contentType)
        if openfile:
            return open(cachefile, 'r')
        else:
            return cachefile

    def clearCache(self, cachefile, inputdata = {}, verb = 'GET'):
        """
        Delete the cache file and the httplib2 cache.
        """
        
        verb = self._verbCheck(verb)
        os.system("/bin/rm -f %s/*" % self['requests']['req_cache_path'])
        cachefile = self.cacheFileName(cachefile, verb, inputdata)
        try:
            os.remove(cachefile)
        except OSError: # File doesn't exist
            return

    def getData(self, cachefile, url, inputdata = {}, incoming_headers = {},
                encoder = True, decoder = True,
                verb = 'GET', contentType = None):
        """
        Takes the already generated *full* path to cachefile and the url of the 
        resource. Don't need to call self.cacheFileName(cachefile, verb, inputdata)
        here.
        """
        verb = self._verbCheck(verb)
        
        # Nested form for version < 2.5 
        
        try:
            # Get the data
            if not inputdata:
                inputdata = self["inputdata"]
            self['logger'].debug('getData: \n\turl: %s\n\tdata: %s' % \
                                 (url, inputdata))
            data, status, reason, from_cache = self["requests"].makeRequest(uri = url,
                                                    verb = verb,
                                                    data = inputdata,
                                                    incoming_headers = incoming_headers,
                                                    encoder = encoder,
                                                    decoder = decoder,
                                                    contentType = contentType)
            if from_cache:
                # If it's coming from the cache we don't need to write it to the
                # second cache, or do we?
                self['logger'].debug('Data is from the httplib2 cache')
            else:
                # Don't need to prepend the cachepath, the methods calling 
                # getData have done that for us 
                f = open(cachefile, 'w')
                f.write(str(data))
                f.close()
        except HTTPException, he:
            if not os.path.exists(cachefile):
                
                msg = 'The cachefile %s does not exist and the service at %s is'
                msg += ' unavailable - it returned %s because %s'
                msg = msg % (cachefile, he.url, he.status, he.reason)
                self['logger'].warning(msg)
                raise HTTPException(msg)
            else:
                cache_age = os.path.getmtime(cachefile)
                t = datetime.datetime.now() - datetime.timedelta(hours = self.get('maxcachereuse', 24))
                cache_dead = cache_age < time.mktime(t.timetuple())                
                if self.get('usestalecache', False) and not cache_dead:
                    # If usestalecache is set the previous version of the cache file 
                    # should be returned, with a suitable message in the log
                    self['logger'].warning('Returning stale cache data')
                    self['logger'].info('%s returned %s because %s' % (he.url, 
                                                                       he.status,
                                                                       he.reason))
                    self['logger'].info('cache file (%s) was created on %s' % (
                                                                        cachefile,
                                                                        cache_age))
                else:
                    if cache_dead:
                        msg = 'The cachefile %s is dead (%s hours older than cache '
                        msg += 'duration), and the service at %s is unavailable - '
                        msg += 'it returned %s because %s'
                        msg = msg % (cachefile, self.get('maxcachereuse', 24), he.url, he.status, he.reason)
                        self['logger'].warning(msg)
                    elif self.get('usestalecache', False) == False:
                        msg = 'The cachefile %s is stale and the service at %s is'
                        msg += ' unavailable - it returned %s because %s'
                        msg = msg % (cachefile, he.url, he.status, he.reason)
                        self['logger'].warning(msg)
                    raise HTTPException(msg)
            
    def _verbCheck(self, verb='GET'):
        if verb.upper() in self.supportVerbList:
            return verb.upper()
        elif self['method'].upper() in self.supportVerbList:
            return self['method'].upper()
        else:
            raise TypeError, 'verb parameter needs to be set'


class TempDirectory():
    """Directory that cleans up after itself"""
    def __init__(self, dir):
        self.dir = dir

    def __del__(self):
        shutil.rmtree(self.dir, ignore_errors = True)
