#!/usr/bin/python
"""
_Requests_

A set of classes to handle making http and https requests to a remote server and
deserialising the response.

The response from the remote server is cached if expires/etags are set.
"""

import urllib
import os
import base64
import httplib2
import socket
import logging
import urlparse
from httplib import HTTPException
import tempfile
import shutil
import stat
from WMCore.Algorithms import Permissions

from WMCore.WMException import WMException
from WMCore.Wrappers import JsonWrapper as json
from WMCore.Wrappers.JsonWrapper import JSONEncoder, JSONDecoder
from WMCore.Wrappers.JsonWrapper.JSONThunker import JSONThunker

def check_server_url(srvurl):
    good_name = srvurl.startswith('http://') or srvurl.startswith('https://')
    if not good_name:
        msg = "You must include http(s):// in your servers address, %s doesn't" % srvurl
        raise ValueError(msg)

class Requests(dict):
    """
    Generic class for sending different types of HTTP Request to a given URL
    """

    def __init__(self, url = 'http://localhost', dict={}):
        """
        url should really be host - TODO fix that when have sufficient code
        coverage and change _getURLOpener if needed
        """
        #set up defaults
        self.setdefault("accept_type", 'text/html')
        self.setdefault("content_type", 'application/x-www-form-urlencoded')
        self.setdefault("host", url)

        # then update with the incoming dict
        self.update(dict)

        self['endpoint_components'] = urlparse.urlparse(self['host'])

        cache_dir = (self.cachePath(dict.get('cachepath'), dict.get('service_name')))
        self["cachepath"] = cache_dir
        self["req_cache_path"] = os.path.join(cache_dir, '.cache')
        self.setdefault("timeout", 30)
        self.setdefault("logger", logging)

        check_server_url(self['host'])
        # and then get the URL opener
        self.setdefault("conn", self._getURLOpener())
        self.additionalHeaders = {}
        return

    def get(self, uri=None, data={}, incoming_headers={},
               encode = True, decode=True, contentType=None):
        """
        GET some data
        """
        return self.makeRequest(uri, data, 'GET', incoming_headers,
                                encode, decode, contentType)

    def post(self, uri=None, data={}, incoming_headers={},
               encode = True, decode=True, contentType=None):
        """
        POST some data
        """
        return self.makeRequest(uri, data, 'POST', incoming_headers,
                                encode, decode, contentType)

    def put(self, uri=None, data={}, incoming_headers={},
               encode = True, decode=True, contentType=None):
        """
        PUT some data
        """
        return self.makeRequest(uri, data, 'PUT', incoming_headers,
                                encode, decode, contentType)

    def delete(self, uri=None, data={}, incoming_headers={},
               encode = True, decode=True, contentType=None):
        """
        DELETE some data
        """
        return self.makeRequest(uri, data, 'DELETE', incoming_headers,
                                encode, decode, contentType)

    def makeRequest(self, uri=None, data={}, verb='GET', incoming_headers={},
                     encoder=True, decoder=True, contentType=None):
        """
        Make a request to the remote database. for a give URI. The type of
        request will determine the action take by the server (be careful with
        DELETE!). Data should be a dictionary of {dataname: datavalue}.

        Returns a tuple of the data from the server, decoded using the
        appropriate method the response status and the response reason, to be
        used in error handling.

        You can override the method to encode/decode your data by passing in an
        encoding/decoding function to this method. Your encoded data must end up
        as a string.

        """
        #TODO: User agent should be:
        # $client/$client_version (CMS) $http_lib/$http_lib_version $os/$os_version ($arch)
        if contentType:
            headers = {"Content-type": contentType,
                   "User-agent": "WMCore.Services.Requests/v001",
                   "Accept": self['accept_type']}
        else:
            headers = {"Content-type": self['content_type'],
                   "User-agent": "WMCore.Services.Requests/v001",
                   "Accept": self['accept_type']}
        encoded_data = ''

        for key in self.additionalHeaders.keys():
            headers[key] = self.additionalHeaders[key]

        #And now overwrite any headers that have been passed into the call:
        headers.update(incoming_headers)

        # httpib2 requires absolute url
        uri = self['host'] + uri

        # If you're posting an attachment, the data might not be a dict
        #   please test against ConfigCache_t if you're unsure.
        #assert type(data) == type({}), \
        #        "makeRequest input data must be a dict (key/value pairs)"

        # There must be a better way to do this...
        def f(): pass

        if verb != 'GET' and data:
            if type(encoder) == type(self.get) or type(encoder) == type(f):
                encoded_data = encoder(data)
            elif encoder == False:
                # Don't encode the data more than we have to
                #  we don't want to URL encode the data blindly,
                #  that breaks POSTing attachments... ConfigCache_t
                #encoded_data = urllib.urlencode(data)
                #  -- Andrew Melo 25/7/09
                encoded_data = data
            else:
                # Either the encoder is set to True or it's junk, so use
                # self.encode
                encoded_data = self.encode(data)
            headers["Content-length"] = len(encoded_data)
        elif verb == 'GET' and data:
            #encode the data as a get string
            uri = "%s?%s" % (uri, urllib.urlencode(data, doseq=True))

        headers["Content-length"] = str(len(encoded_data))

        assert type(encoded_data) == type('string'), \
                    "Data in makeRequest is %s and not encoded to a string" % type(encoded_data)

        # httplib2 will allow sockets to close on remote end without retrying
        # try to send request - if this fails try again - should then succeed
        try:
            response, result = self['conn'].request(uri, method = verb,
                                    body = encoded_data, headers = headers)
            if response.status == 408: # timeout can indicate a socket error
                response, result = self['conn'].request(uri, method = verb,
                                    body = encoded_data, headers = headers)
        except (socket.error, AttributeError):
            # AttributeError implies initial connection error - need to close
            # & retry. httplib2 doesn't clear httplib state before next request
            # if this is threaded this may spoil things
            # only have one endpoint so don't need to determine which to shut
            [conn.close() for conn in self['conn'].connections.values()]
            self['conn'] = httplib2.Http(self["req_cache_path"])
            # ... try again... if this fails propagate error to client
            try:
                response, result = self['conn'].request(uri, method = verb,
                                    body = encoded_data, headers = headers)
            except AttributeError:
                # socket/httplib really screwed up - nuclear option
                self['conn'].connections = {}
                raise socket.error, 'Error contacting: %s' % self.getDomainName()
        if response.status >= 400:
            e = HTTPException()
            setattr(e, 'req_data', encoded_data)
            setattr(e, 'req_headers', headers)
            setattr(e, 'url', uri)
            setattr(e, 'result', result)
            setattr(e, 'status', response.status)
            setattr(e, 'reason', response.reason)
            setattr(e, 'headers', response)
            raise e

        if type(decoder) == type(self.makeRequest) or type(decoder) == type(f):
            result = decoder(result)
        elif decoder != False:
            result = self.decode(result)
        #TODO: maybe just return result and response...
        return result, response.status, response.reason, response.fromcache

    def encode(self, data):
        """
        encode data into some appropriate format, for now make it a string...
        """
        return urllib.urlencode(data, doseq=1)

    def decode(self, data):
        """
        decode data to some appropriate format, for now make it a string...
        """
        return data.__str__()

    def _getURLOpener(self):
        """
        method getting an HTTPConnection, it is used by the constructor such
        that a sub class can override it to have different type of connection
        i.e. - if it needs authentication, or some fancy handler
        """
        return httplib2.Http(self['req_cache_path'], self['timeout'])

    def cachePath(self, given_path, service_name):
        """Return cache location"""
        if not service_name:
            service_name = 'REQUESTS'
        top = self.cacheTopPath(given_path, service_name)

        # deal with multiple Services that have the same service running and
        # with multiple users for a given Service
        if self.getUserName() is None:
            cachepath = os.path.join(top, self['endpoint_components'].netloc)
        else:
            cachepath = os.path.join(top, '%s-%s' % (self.getUserName(), self.getDomainName()))

        try:
            # only we should be able to write to this dir
            os.makedirs(cachepath, stat.S_IRWXU)
        except OSError:
            if not os.path.isdir(cachepath):
                raise
            Permissions.owner_readwriteexec(cachepath)

        return cachepath

    def cacheTopPath(self, given_path, service_name):
        """Where to cache results?

        Logic:
          o If passed in take that
          o Is the environment variable "SERVICE_NAME"_CACHE_DIR defined?
          o Is WMCORE_CACHE_DIR set
          o Generate a temporary directory
          """
        if given_path:
            return given_path
        user = str(os.getuid())
        # append user id so users don't clobber each other
        lastbit = os.path.join('.wmcore_cache_%s' % user, service_name.lower())
        for var in ('%s_CACHE_DIR' % service_name.upper(),
                    'WMCORE_CACHE_DIR'):
            if os.environ.get(var):
                firstbit = os.environ[var]
                break
        else:
            dir = tempfile.mkdtemp(prefix='.wmcore_cache_')
            # object to store temporary directory - cleaned up on destruction
            self['deleteCacheOnExit'] = TempDirectory(dir)
            return dir

        return os.path.join(firstbit, lastbit)

    def getDomainName(self):
        """Parse netloc info to get hostname"""
        return self['endpoint_components'].hostname

    def getUserName(self):
        """Parse netloc to get user"""
        return self['endpoint_components'].username

class JSONRequests(Requests):
    """
    Example implementation of Requests that encodes data to/from JSON.
    """
    def __init__(self, url = 'http://localhost:8080', dict={}):
        Requests.__init__(self, url, dict)
        self['accept_type'] = "application/json"
        self['content_type'] = "application/json"

    def encode(self, data):
        """
        encode data as json
        """
        encoder = JSONEncoder()
        thunker = JSONThunker()
        thunked = thunker.thunk(data)
        return encoder.encode(thunked)


    def decode(self, data):
        """
        decode the data to python from json
        """
        if data:
            decoder = JSONDecoder()
            thunker = JSONThunker()
            data =  decoder.decode(data)
            unthunked = thunker.unthunk(data)
            return unthunked
        else:
            return {}

class BasicAuthJSONRequests(JSONRequests):
    """
    _BasicAuthJSONRequests_

    Support basic HTTP auth for JSON requests.  The username and password must
    be embedded into the url in the following form:
        username:password@hostname
    """
    def __init__(self, url = "http://localhost:8080", dict={}):
        endpoint_components = urlparse.urlparse(url)
        # Cleanly pull out the user/password from the url
        if endpoint_components.port:
            netloc = '%s:%s' % (endpoint_components.hostname,
                        endpoint_components.port)
        else:
            netloc = endpoint_components.hostname

        #Build a URL without the username/password information
        url = urlparse.urlunparse(
                [endpoint_components.scheme,
                 netloc,
                 endpoint_components.path,
                 endpoint_components.params,
                 endpoint_components.query,
                 endpoint_components.fragment])

        JSONRequests.__init__(self, url, dict)
        # Add the necessary auth information into the header
        auth_string = "Basic %s" % base64.encodestring('%s:%s' % (endpoint_components.username,
                                                                  endpoint_components.password)).strip()
        if endpoint_components.username != None:
            self.additionalHeaders["Authorization"] = auth_string
        return

class SSLRequests(Requests):
    """
    Implementation of Requests using HTTPS to send requests to a given URL,
    without authenticating via a key/cert pair.
    """
    def _getURLOpener(self):
        """
        method getting a secure (HTTPS) connection
        """
        return httplib2.Http(self['req_cache_path'], self['timeout'])

class SSLJSONRequests(JSONRequests):
    """
    _SSLJSONRequests_

    Implementation of JSONRequests using HTTPS to send requests to a given URL,
    without authenticating via a key/cert pair.
    """
    def _getURLOpener(self):
        """
        _getURLOpener_

        Retrieve a secure (HTTPS) connection.
        """
        return httplib2.Http(self['req_cache_path'], self['timeout'])


class SecureRequests(Requests):
    """
    Implementation of Requests using a different connection type, e.g. use HTTPS
    to send requests to a given URL, authenticating via a key/cert pair
    """
    def _getURLOpener(self):
        """
        method getting a secure (HTTPS) connection
        """
        # if we have a key/cert add to request, if not proceed as not all https connections require them
        key, cert = None, None
        try:
            key, cert = self.getKeyCert()
        except Exception, ex:
            self['logger'].warning('No certificate or key found, authentication may fail')
            self['logger'].debug(str(ex))

        http = httplib2.Http(self['req_cache_path'], self['timeout'])

        # Domain must be just a hostname and port. self[host] is a URL currently
        if key or cert:
            http.add_certificate(key=key, cert=cert, domain=self.getDomainName())
        return http

    def getKeyCert(self):
        """
       _getKeyCert_

       Gets the User Proxy if it exists, otherwise throws an exception.
       This code is borrowed from DBSAPI/dbsHttpService.py
        """
        # Zeroth case is if the class has over ridden the key/cert and has it
        # stored in self
        if self.has_key('cert') and self.has_key('key' ) \
             and self['cert'] and self['key']:
            key = self['key']
            cert = self['cert']

        # Now we're trying to guess what the right cert/key combo is...
        # First presendence to HOST Certificate, This is how it set in Tier0
        elif os.environ.has_key('X509_HOST_CERT'):
            cert = os.environ['X509_HOST_CERT']
            key = os.environ['X509_HOST_KEY']
        # Second preference to User Proxy, very common
        elif (os.environ.has_key('X509_USER_PROXY')) and \
                (os.path.exists( os.environ['X509_USER_PROXY'])):
            cert = os.environ['X509_USER_PROXY']
            key = cert

        # Third preference to User Cert/Proxy combinition
        elif os.environ.has_key('X509_USER_CERT'):
            cert = os.environ['X509_USER_CERT']
            key = os.environ['X509_USER_KEY']

        # TODO: only in linux, unix case, add other os case
        # look for proxy at default location /tmp/x509up_u$uid
        elif os.path.exists('/tmp/x509up_u'+str(os.getuid())):
            cert = '/tmp/x509up_u'+str(os.getuid())
            key = cert

        # Worst case, hope the user has a cert in ~/.globus
        else :
            cert = os.environ['HOME'] + '/.globus/usercert.pem'
            if os.path.exists(os.environ['HOME'] + '/.globus/userkey.pem'):
                key = os.environ['HOME'] + '/.globus/userkey.pem'
            else:
                key = cert

        #Set but not found
        if not os.path.exists(cert) or not os.path.exists(key):
            raise WMException('Request requires a host certificate and key',
                              "WMCORE-11")

        # All looks OK, still doesn't guarantee proxy's validity etc.
        return key, cert

class TempDirectory():
    """Directory that cleans up after itself"""
    def __init__(self, dir):
        self.dir = dir

    def __del__(self):
        shutil.rmtree(self.dir, ignore_errors = True)
