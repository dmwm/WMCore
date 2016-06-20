#!/usr/bin/env python
#pylint: disable=C0103,R0913,W0102
"""
_Requests_

A set of classes to handle making http and https requests to a remote server and
deserialising the response.

The response from the remote server is cached if expires/etags are set.
"""

import base64
import logging
import os
import shutil
import socket
import stat
import sys
import tempfile
import traceback
import urllib
import urlparse
from httplib import HTTPException
from json import JSONEncoder, JSONDecoder

try:
    import cStringIO as StringIO
except ImportError:
    import StringIO

from WMCore.Algorithms import Permissions

from WMCore.WMException import WMException
from WMCore.Wrappers.JsonWrapper.JSONThunker import JSONThunker
try:
    from WMCore.Services.pycurl_manager import RequestHandler, ResponseHeader
except ImportError:
    pass
from WMCore.Lexicon import sanitizeURL

def check_server_url(srvurl):
    """Check given url for correctness"""
    good_name = srvurl.startswith('http://') or srvurl.startswith('https://')
    if not good_name:
        msg  = "You must include "
        msg += "http(s):// in your server's address, %s doesn't" % srvurl
        raise ValueError(msg)


class Requests(dict):
    """
    Generic class for sending different types of HTTP Request to a given URL
    """

    def __init__(self, url = 'http://localhost', idict=None):
        """
        url should really be host - TODO fix that when have sufficient code
        coverage and change _getURLOpener if needed
        """
        if  not idict:
            idict = {}
        dict.__init__(self, idict)
        self.pycurl = idict.get('pycurl', None)
        self.capath = idict.get('capath', None)
        if self.pycurl:
            self.reqmgr = RequestHandler()

        #set up defaults
        self.setdefault("accept_type", 'text/html')
        self.setdefault("content_type", 'application/x-www-form-urlencoded')
        self.additionalHeaders = {}

        # check for basic auth early, as if found this changes the url
        urlComponent = sanitizeURL(url)
        if urlComponent['username'] is not None:
            self.addBasicAuth(\
                urlComponent['username'], urlComponent['password'])
            url = urlComponent['url'] # remove user, password from url

        self.setdefault("host", url)

        # then update with the incoming dict
        self.update(idict)

        self['endpoint_components'] = urlparse.urlparse(self['host'])

        # If cachepath = None disable caching
        if 'cachepath' in idict and idict['cachepath'] is None:
            self["req_cache_path"] = None
        else:
            cache_dir = (self.cachePath(idict.get('cachepath'), \
                        idict.get('service_name')))
            self["cachepath"] = cache_dir
            self["req_cache_path"] = os.path.join(cache_dir, '.cache')
        self.setdefault("timeout", 300)
        self.setdefault("logger", logging)

        check_server_url(self['host'])
        if not self.pycurl:
            # and then get the URL opener
            self.setdefault("conn", self._getURLOpener())


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
        Wrapper around request helper functions.
        """

        if  self.pycurl:
            result = self.makeRequest_pycurl(uri, data, verb, incoming_headers,
                         encoder, decoder, contentType)
        else:
            result = self.makeRequest_httplib(uri, data, verb, incoming_headers,
                         encoder, decoder, contentType)
        return result

    def makeRequest_pycurl(self, uri=None, params={}, verb='GET',
            incoming_headers={}, encoder=True, decoder=True, contentType=None):
        """
        Make HTTP(s) request via pycurl library. Stay complaint with
        makeRequest_httplib method.
        """
        ckey, cert = self.getKeyCert()
        capath = self.getCAPath()
        if  not contentType:
            contentType = self['content_type']
        headers = {"Content-type": contentType,
               "User-agent": "WMCore.Services.Requests/v001",
               "Accept": self['accept_type']}
        for key in self.additionalHeaders.keys():
            headers[key] = self.additionalHeaders[key]
        #And now overwrite any headers that have been passed into the call:
        headers.update(incoming_headers)
        url = self['host'] + uri
        response, data = self.reqmgr.request(url, params, headers, \
                    verb=verb, ckey=ckey, cert=cert, capath=capath, decode=decoder)
        return data, response.status, response.reason, response.fromcache

    def makeRequest_httplib(self, uri=None, data={}, verb='GET',
            incoming_headers={}, encoder=True, decoder=True, contentType=None):
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
        #do not add a dependency to httplib2 if we are using pycurl


        #TODO: User agent should be:
        # $client/$client_version (CMS)
        # $http_lib/$http_lib_version $os/$os_version ($arch)
        if  not contentType:
            contentType = self['content_type']
        headers = {"Content-type": contentType,
               "User-agent": "WMCore.Services.Requests/v001",
               "Accept": self['accept_type']}
        encoded_data = ''

        for key in self.additionalHeaders.keys():
            headers[key] = self.additionalHeaders[key]

        #And now overwrite any headers that have been passed into the call:
        #WARNING: doesn't work with deplate so only accept gzip
        incoming_headers["accept-encoding"] = "gzip,identity"
        headers.update(incoming_headers)

        # httpib2 requires absolute url
        uri = self['host'] + uri

        # If you're posting an attachment, the data might not be a dict
        #   please test against ConfigCache_t if you're unsure.
        #assert type(data) == type({}), \
        #        "makeRequest input data must be a dict (key/value pairs)"

        # There must be a better way to do this...
        def f():
            """Dummy function"""
            pass

        if verb != 'GET' and data:
            if isinstance(encoder, type(self.get)) or isinstance(encoder, type(f)):
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

        assert isinstance(encoded_data, type('string')), \
            "Data in makeRequest is %s and not encoded to a string" \
                % type(encoded_data)

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
            for conn in self['conn'].connections.values():
                conn.close()
            self['conn'] = self._getURLOpener()
            # ... try again... if this fails propagate error to client
            try:
                response, result = self['conn'].request(uri, method = verb,
                                    body = encoded_data, headers = headers)
            except AttributeError as ex:
                msg = traceback.format_exc()
                # socket/httplib really screwed up - nuclear option
                self['conn'].connections = {}
                raise socket.error('Error contacting: %s: %s' \
                        % (self.getDomainName(), msg))
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

        if isinstance(decoder, type(self.makeRequest)) or isinstance(decoder, type(f)):
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
            cachepath = os.path.join(top, '%s-%s' \
                % (self.getUserName(), self.getDomainName()))

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
            idir = tempfile.mkdtemp(prefix='.wmcore_cache_')
            # object to store temporary directory - cleaned up on destruction
            self['deleteCacheOnExit'] = TempDirectory(idir)
            return idir

        return os.path.join(firstbit, lastbit)

    def getDomainName(self):
        """Parse netloc info to get hostname"""
        return self['endpoint_components'].hostname

    def getUserName(self):
        """Parse netloc to get user"""
        return self['endpoint_components'].username

    def _getURLOpener(self):
        """
        method getting a secure (HTTPS) connection
        """
        import httplib2

        key, cert = None, None
        if self['endpoint_components'].scheme == 'https':
            # only add certs to https requests
            # if we have a key/cert add to request,
            # if not proceed as not all https connections require them
            try:
                key, cert = self.getKeyCert()
            except Exception as ex: #pylint: disable=broad-except
                msg = 'No certificate or key found, authentication may fail'
                self['logger'].info(msg)
                self['logger'].debug(str(ex))

        try:
            # disable validation as we don't have a single PEM with all ca's
            http = httplib2.Http(self['req_cache_path'], self['timeout'],
                                 disable_ssl_certificate_validation = True)
        except TypeError:
            # old httplib2 versions disable validation by default
            http = httplib2.Http(self['req_cache_path'], self['timeout'])

        # Domain must be just a hostname and port. self[host] is a URL currently
        if key or cert:
            http.add_certificate(key=key, cert=cert, domain='')
        return http

    def addBasicAuth(self, username, password):
        """Add basic auth headers to request"""
        auth_string = "Basic %s" % base64.encodestring('%s:%s' % (
                                            username, password)).strip()
        self.additionalHeaders["Authorization"] = auth_string

    def getKeyCert(self):
        """
       _getKeyCert_

       Get the user credentials if they exist, otherwise throw an exception.
       This code was modified from DBSAPI/dbsHttpService.py
        """
        cert = None
        key = None
        # Zeroth case is if the class has over ridden the key/cert and has it
        # stored in self
        if 'cert' in self and 'key' in self \
             and self['cert'] and self['key']:
            key = self['key']
            cert = self['cert']

        # Now we're trying to guess what the right cert/key combo is...
        # First preference to HOST Certificate, This is how it set in Tier0
        elif 'X509_HOST_CERT' in os.environ:
            cert = os.environ['X509_HOST_CERT']
            key = os.environ['X509_HOST_KEY']
        # Second preference to User Proxy, very common
        elif ('X509_USER_PROXY' in os.environ) and \
                (os.path.exists( os.environ['X509_USER_PROXY'])):
            cert = os.environ['X509_USER_PROXY']
            key = cert

        # Third preference to User Cert/Proxy combinition
        elif 'X509_USER_CERT' in os.environ:
            cert = os.environ['X509_USER_CERT']
            key = os.environ['X509_USER_KEY']

        # TODO: only in linux, unix case, add other os case
        # look for proxy at default location /tmp/x509up_u$uid
        elif os.path.exists('/tmp/x509up_u'+str(os.getuid())):
            cert = '/tmp/x509up_u'+str(os.getuid())
            key = cert

        # if interactive we can use an encrypted certificate
        elif sys.stdin.isatty():
            if os.path.exists(os.environ['HOME'] + '/.globus/usercert.pem'):
                cert = os.environ['HOME'] + '/.globus/usercert.pem'
                if os.path.exists(os.environ['HOME'] + '/.globus/userkey.pem'):
                    key = os.environ['HOME'] + '/.globus/userkey.pem'
                else:
                    key = cert

        #Set but not found
        if  key and cert:
            if not os.path.exists(cert) or not os.path.exists(key):
                raise WMException('Request requires a host certificate and key',
                                  "WMCORE-11")

        # All looks OK, still doesn't guarantee proxy's validity etc.
        return key, cert

    def getCAPath(self):
        """
        _getCAPath_

        Return the path of the CA certificates. The check is loose in the pycurl_manager:
        is capath == None then the server identity is not verified. To enable this check
        you need to set either the X509_CERT_DIR variable or the cacert key of the request.
        """
        cacert = None
        if 'capath' in self:
            cacert = self['capath']
        elif "X509_CERT_DIR" in os.environ:
            cacert = os.environ["X509_CERT_DIR"]
        return cacert

    def uploadFile(self, fileName, url, fieldName = 'file1', params = [], verb = 'POST'):
        """
        Upload a file with curl streaming it directly from disk
        """
        ckey, cert = self.getKeyCert()
        capath = self.getCAPath()
        import pycurl
        c = pycurl.Curl()
        if verb == 'POST':
            c.setopt(c.POST, 1)
        elif verb == 'PUT':
            c.setopt(pycurl.CUSTOMREQUEST, 'PUT')
        else:
            raise HTTPException("Verb %s not sopported for upload." % verb)
        c.setopt(c.URL, url)
        fullParams = [(fieldName, (c.FORM_FILE, fileName))]
        fullParams.extend(params)
        c.setopt(c.HTTPPOST, fullParams)
        bbuf = StringIO.StringIO()
        hbuf = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, bbuf.write)
        c.setopt(pycurl.HEADERFUNCTION, hbuf.write)
        if  capath:
            c.setopt(pycurl.CAPATH, capath)
            c.setopt(pycurl.SSL_VERIFYPEER, True)
        else:
            c.setopt(pycurl.SSL_VERIFYPEER, False)
        if  ckey:
            c.setopt(pycurl.SSLKEY, ckey)
        if  cert:
            c.setopt(pycurl.SSLCERT, cert)
        c.perform()
        hres = hbuf.getvalue()
        bres = bbuf.getvalue()
        rh = ResponseHeader(hres)
        c.close()
        if  rh.status < 200 or rh.status >= 300:
            exc = HTTPException(bres)
            setattr(exc, 'req_data', fullParams)
            setattr(exc, 'url', url)
            setattr(exc, 'result', bres)
            setattr(exc, 'status', rh.status)
            setattr(exc, 'reason', rh.reason)
            setattr(exc, 'headers', rh.header)
            raise exc

        return bres

    def downloadFile(self, fileName, url):
        """
        Download a file with curl streaming it directly to disk
        """
        ckey, cert = self.getKeyCert()
        capath = self.getCAPath()
        import pycurl
        from WMCore.Services.pycurl_manager import ResponseHeader

        hbuf = StringIO.StringIO()

        with open(fileName, "wb") as fp:
            curl = pycurl.Curl()
            curl.setopt(pycurl.URL, url)
            curl.setopt(pycurl.WRITEDATA, fp)
            curl.setopt(pycurl.HEADERFUNCTION, hbuf.write)
            if  capath:
                curl.setopt(pycurl.CAPATH, capath)
                curl.setopt(pycurl.SSL_VERIFYPEER, True)
            else:
                curl.setopt(pycurl.SSL_VERIFYPEER, False)
            if  ckey:
                curl.setopt(pycurl.SSLKEY, ckey)
            if  cert:
                curl.setopt(pycurl.SSLCERT, cert)
            curl.setopt(pycurl.FOLLOWLOCATION, 1)
            curl.perform()
            curl.close()

            header = ResponseHeader(hbuf.getvalue())
            if header.status < 200 or header.status >= 300:
                raise RuntimeError('Reading %s failed with code %s' % (url, header.status))
        return fileName, header



class JSONRequests(Requests):
    """
    Example implementation of Requests that encodes data to/from JSON.
    """
    def __init__(self, url = 'http://localhost:8080', idict={}):
        Requests.__init__(self, url, idict)
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


class TempDirectory():
    """Directory that cleans up after itself"""
    def __init__(self, idir):
        self.dir = idir

    def __del__(self):
        if shutil:
            shutil.rmtree(self.dir, ignore_errors = True)
