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
of 300 seconds. Over ride this by passing in a timeout via the configuration
dict, set to None if you want to turn off the timeout.

If you just want to retrieve the data without caching use the Requests class
directly.

The Service class provides two layers of caching:
    1. Caching from httplib2 is provided via Request, this respects etag and
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

from builtins import str
from future import standard_library
standard_library.install_aliases()

import datetime
import json
import logging
import os
import time
from io import BytesIO, StringIO
from http.client import HTTPException

from Utils.PythonVersion import PY3
from WMCore.Services.Requests import Requests, JSONRequests
from WMCore.WMException import WMException

try:
    from httplib2 import HttpLib2Error
except ImportError:
    # Mock HttpLib2Error since we don't want that WMCore depend on httplib2 using pycurl
    class HttpLib2Error(Exception):
        pass


def isfile(obj):
    """
    Check whether obj is a file-like object (file, StringIO)"
    """
    return hasattr(obj, 'flush')


def cache_expired(cache, delta=0):
    """
    Is the cache expired? At delta hours (default 0) in the future.
    """
    if isfile(cache):
        # already opened file-like object, assume we need to refresh
        return True
    else:
        # then it's a file name
        if not os.path.exists(cache):
            return True

        delta = datetime.timedelta(hours=delta)
        t = datetime.datetime.now() - delta
        # cache file mtime has been set to cache expiry time
        if os.path.getmtime(cache) < time.mktime(t.timetuple()):
            return True

    return False


def _makeHash(inputdata):
    """
    Turn the input data into json and hash the string. This is simple and
    means that the input data must be json-serialisable, which is good.
    """
    json_hash = json.dumps(inputdata)
    return json_hash.__hash__()


class Service(dict):
    def __init__(self, cfg_dict=None):
        super(Service, self).__init__()
        cfg_dict = cfg_dict or {}
        # The following should read the configuration class
        for a in ['endpoint']:
            assert a in list(cfg_dict), "Can't have a service without a %s" % a

        # if end point ends without '/', add that
        if not cfg_dict['endpoint'].endswith('/'):
            cfg_dict['endpoint'] = cfg_dict['endpoint'].strip() + '/'

        # set up defaults
        self.setdefault("inputdata", {})
        self.setdefault("cacheduration", 0.5)
        self.supportVerbList = ('GET', 'POST', 'PUT', 'DELETE')
        # this value should be only set when whole service class uses
        # the same verb ('GET', 'POST', 'PUT', 'DELETE')
        self.setdefault("method", None)

        # Set a timeout for the socket
        self.setdefault("timeout", 300)

        # then update with the incoming dict
        self.update(cfg_dict)

        self['service_name'] = self.__class__.__name__  # used for cache naming

        # Get the request class, to instantiate later
        # either passed as param to __init__, determine via scheme or default
        if isinstance(self.get('requests'), type):
            requests = self['requests']
        elif self.get('accept_type') == "application/json" and self.get('content_type') == "application/json":
            requests = JSONRequests
        else:
            requests = Requests
        # Instantiate a Request
        try:
            self["requests"] = requests(cfg_dict['endpoint'], cfg_dict)
        except WMException as ex:
            msg = str(ex)
            self["logger"].exception(msg)
            raise

        # cachepath will be modified - i.e. hostname added
        self['cachepath'] = self["requests"]["cachepath"]

        if 'logger' not in self:
            if self['cachepath']:
                logfile = os.path.join(self['cachepath'], '%s.log' % self.__class__.__name__.lower())
            else:
                logfile = None
            logging.basicConfig(level=logging.DEBUG,
                                format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                                datefmt='%m-%d %H:%M',
                                filename=logfile,
                                filemode='w')
            self['logger'] = logging.getLogger(self.__class__.__name__)
            self['requests']['logger'] = self['logger']

        msg = "Service '%s' initialized with the following settings: %s"
        self['logger'].debug(msg, self.__class__.__name__, self)

    def cacheFileName(self, cachefile, verb='GET', inputdata=None):
        """
        Calculate the cache filename for a given query.
        """
        # if not caching to disk return StringIO object
        if not self['cachepath'] or not cachefile:
            return StringIO() if PY3 else BytesIO()

        inputdata = inputdata or {}
        if inputdata:
            hash_ = _makeHash(inputdata)
        else:
            hash_ = _makeHash(self['inputdata'])
        cachefile = "%s/%s_%s_%s" % (self["cachepath"], hash_, verb, cachefile)

        return cachefile

    def refreshCache(self, cachefile, url='', inputdata=None, openfile=True,
                     encoder=True, decoder=True, verb='GET', contentType=None, incoming_headers=None, binary=False):
        """
        See if the cache has expired. If it has make a new request to the
        service for the input data. Return the cachefile as an open file object.
        If cachefile is None returns StringIO.

        :param cachefile: cache file name
        :param url: url value
        :param inputdata: HTTP (JSON) payload string
        :param openfile: flag to use open file, boolean
        :param encoder: flag to use encoder, boolean
        :param decoder: flag to use decode, boolean
        :param verb: HTTP method, string
        :param contentType: HTTP content type value, string
        :param incoming_headers: set of HTTP headers, dict
        :param binary: use binary mode to decode HTTP data, boolean
        :return: either file object or StringIO
        """
        inputdata = inputdata or {}
        incoming_headers = incoming_headers or {}
        verb = self._verbCheck(verb)

        cachefile = self.cacheFileName(cachefile, verb, inputdata)

        if cache_expired(cachefile, self["cacheduration"]):
            self.getData(cachefile, url, inputdata, incoming_headers, encoder, decoder, verb, contentType, binary=binary)
        else:
            self['logger'].debug('Data is from the Service cache')

        # cachefile may be filename or file object
        if openfile and not isfile(cachefile):
            if binary:
                return open(cachefile, 'rb')
            return open(cachefile, 'r', encoding='utf-8')
        else:
            return cachefile

    def forceRefresh(self, cachefile, url='', inputdata=None, openfile=True,
                     encoder=True, decoder=True, verb='GET',
                     contentType=None, incoming_headers=None, binary=False):
        """
        Make a new request to the service for the input data, regardless of the
        cache state. Return the cachefile as an open file object.
        If cachefile is None returns StringIO.

        :param cachefile: cache file name
        :param url: url value
        :param inputdata: HTTP (JSON) payload string
        :param openfile: flag to use open file, boolean
        :param encoder: flag to use encoder, boolean
        :param decoder: flag to use decode, boolean
        :param verb: HTTP method, string
        :param contentType: HTTP content type value, string
        :param incoming_headers: set of HTTP headers, dict
        :param binary: use binary mode to decode HTTP data, boolean
        :return: either file object or StringIO
        """
        inputdata = inputdata or {}
        incoming_headers = incoming_headers or {}
        verb = self._verbCheck(verb)

        cachefile = self.cacheFileName(cachefile, verb, inputdata)

        self['logger'].debug("Forcing cache refresh of %s" % cachefile)
        incoming_headers.update({'cache-control': 'no-cache'})
        self.getData(cachefile, url, inputdata, incoming_headers,
                     encoder, decoder, verb, contentType, force_refresh=True, binary=binary)
        if openfile and not isfile(cachefile):
            if binary:
                return open(cachefile, 'rb')
            return open(cachefile, 'r', encoding='utf-8')
        else:
            return cachefile

    def clearCache(self, cachefile, inputdata=None, verb='GET'):
        """
        Delete the cache file and the httplib2 cache.
        """
        if not self['cachepath'] or not cachefile:
            # nothing to clear
            return

        inputdata = inputdata or {}
        verb = self._verbCheck(verb)
        os.system("/bin/rm -f %s/*" % self['requests']['req_cache_path'])
        cachefile = self.cacheFileName(cachefile, verb, inputdata)
        try:
            if not isfile(cachefile):
                os.remove(cachefile)
        except OSError:  # File doesn't exist
            return

    def getData(self, cachefile, url, inputdata=None, incoming_headers=None,
                encoder=True, decoder=True,
                verb='GET', contentType=None, force_refresh=False, binary=False):
        """
        Takes the already generated *full* path to cachefile and the url of the
        resource. Don't need to call self.cacheFileName(cachefile, verb, inputdata)
        here.

        If cachefile is StringIO append to that

        :param cachefile: cache file name
        :param url: url value
        :param inputdata: HTTP (JSON) payload string
        :param encoder: flag to use encoder, boolean
        :param decoder: flag to use decode, boolean
        :param verb: HTTP method, string
        :param contentType: HTTP content type value, string
        :param force_refresh: refresh internal cache, boolean
        :param binary: use binary mode to decode HTTP data, boolean
        :return: None
        """
        inputdata = inputdata or {}
        incoming_headers = incoming_headers or {}
        verb = self._verbCheck(verb)

        try:
            # Get the data
            if not inputdata:
                inputdata = self["inputdata"]
            self['logger'].debug('getData: \n\turl: %s\n\tverb: %s\n\tincoming_headers: %s\n\tdata: %s',
                                 url, verb, incoming_headers, inputdata)
            # self['logger'].debug('getData: \n\turl: %s\n\tdata: %s' % \
            #                     (url, inputdata))
            data, dummyStatus, dummyReason, from_cache = self["requests"].makeRequest(uri=url,
                                                                                      verb=verb,
                                                                                      data=inputdata,
                                                                                      incoming_headers=incoming_headers,
                                                                                      encoder=encoder,
                                                                                      decoder=decoder,
                                                                                      contentType=contentType)
            if from_cache:
                # If it's coming from the cache we don't need to write it to the
                # second cache, or do we?
                self['logger'].debug('Data is from the Requests cache')
            else:
                # getData have done that for us
                if isfile(cachefile):
                    cachefile.write(data)
                    cachefile.seek(0, 0)  # return to beginning of file
                elif binary:
                    with open(cachefile, 'wb') as f:
                        f.write(data)
                elif isinstance(data, (dict, list)):
                    with open(cachefile, 'w', encoding='utf-8') as f:
                        f.write(json.dumps(data))
                else:
                    with open(cachefile, 'w', encoding='utf-8') as f:
                        f.write(data)


        except (IOError, HttpLib2Error, HTTPException) as he:
            #
            # Overly complicated exception handling. This is due to a request
            # from *Ops that it is very clear that data is is being returned
            # from a cachefile, and that cachefiles can be good/stale/dead.
            #
            if force_refresh or isfile(cachefile) or not os.path.exists(cachefile):
                msg = 'The cachefile %s does not exist and the service at %s'
                msg = msg % (cachefile, self["requests"]['host'] + url)
                if hasattr(he, 'status') and hasattr(he, 'reason'):
                    msg += ' is unavailable - it returned %s because %s' % (he.status,
                                                                            he.reason)
                    if hasattr(he, 'result'):
                        msg += ' with result: %s\n' % he.result
                else:
                    msg += ' raised a %s when accessed' % he.__repr__()
                self['logger'].warning(msg)
                raise he
            else:
                cache_dead = cache_expired(cachefile, delta=self["cacheduration"])
                if cache_dead:
                    msg = 'The cachefile %s is dead (older than %s hours of cache duration), '
                    msg += 'and the service at %s '
                    msg = msg % (cachefile, self["cacheduration"], url)
                    if hasattr(he, 'status') and hasattr(he, 'reason'):
                        msg += 'is unavailable - it returned %s because %s' % (he.status, he.reason)
                    else:
                        msg += 'raised a %s when accessed' % he.__repr__()
                    self['logger'].warning(msg)
                    raise he
                if self.get('usestalecache', False):
                    # then we can return data from the cache file, without raising an exception
                    # but with a suitable message in the log
                    msg = 'Returning stale cache data from %s, the service at ' % cachefile
                    if hasattr(he, 'status') and hasattr(he, 'reason'):
                        msg += '%s returned %s because %s' % (he.url, he.status, he.reason)
                    else:
                        msg += '%s raised a %s when accessed' % (url, he.__repr__())
                    self['logger'].warning(msg)
                else:
                    # Cache is not dead, but Service is configured to not return stale data.
                    msg = 'The cachefile %s is stale and the service at %s ' % (cachefile, url)
                    if hasattr(he, 'status') and hasattr(he, 'reason'):
                        msg += 'is unavailable - it returned %s because %s' % (he.status, he.reason)
                    else:
                        msg += 'raised a %s when accessed' % he.__repr__()
                    self['logger'].warning(msg)
                    raise he

    def _verbCheck(self, verb='GET'):
        if verb.upper() in self.supportVerbList:
            return verb.upper()
        elif self['method'].upper() in self.supportVerbList:
            return self['method'].upper()
        else:
            raise TypeError('verb parameter needs to be set')
