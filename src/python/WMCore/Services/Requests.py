#!/usr/bin/env python
# pylint: disable=C0103,R0913,W0102
"""
_Requests_

A set of classes to handle making http and https requests to a remote server and
deserialising the response.

The response from the remote server is cached if expires/etags are set.
Note that the cache can have two different behaviors:
1. when WMCORE_CACHE_DIR is defined: it defines one specific path to be used for the
   cache files. Cache files are never automatically cleaned up and it's up to the user/
   maintainer to do so. Note that cache directories are named after the base class.
2. otherwise, the system will use the Operating System temp dir to store the cache files,
   which uses `/tmp` as default for Linux systems. When using these temporary areas, the
   cache files are automatically cleaned up when the object using it is destroyed and
   garbage collected. Cache directories carry a random name.

By default, all of the WMCore central services define the CACHE_DIR (to use /data/srv/state).
"""
from __future__ import division, print_function

from future import standard_library
standard_library.install_aliases()

from builtins import str, bytes, object
from future.utils import viewvalues

import base64
import logging
import os
import shutil
import socket
import stat
import tempfile
import traceback
import types

from urllib.parse import urlparse, urlencode
from io import BytesIO
from http.client import HTTPException
from json import JSONEncoder, JSONDecoder

from Utils.CertTools import getKeyCertFromEnv, getCAPathFromEnv
from Utils.Utilities import encodeUnicodeToBytes, decodeBytesToUnicode
from Utils.PythonVersion import PY3
from WMCore.Algorithms import Permissions
from WMCore.Lexicon import sanitizeURL
from WMCore.WMException import WMException
from WMCore.Wrappers.JsonWrapper.JSONThunker import JSONThunker
from Utils.PortForward import portForward

try:
    from WMCore.Services.pycurl_manager import RequestHandler, ResponseHeader
except ImportError:
    pass

try:
    from httplib2 import ServerNotFoundError
except ImportError:
    # Mock ServerNotFoundError since we don't want that WMCore depend on httplib2 using pycurl
    class ServerNotFoundError(Exception):
        pass


def check_server_url(srvurl):
    """Check given url for correctness"""
    good_name = srvurl.startswith('http://') or srvurl.startswith('https://')
    if not good_name:
        msg = "You must include "
        msg += "http(s):// in your server's address, %s doesn't" % srvurl
        raise ValueError(msg)


class Requests(dict):
    """
    Generic class for sending different types of HTTP Request to a given URL
    """

    @portForward(8443)
    def __init__(self, url='http://localhost', idict=None):
        """
        url should really be host - TODO fix that when have sufficient code
        coverage and change _getURLOpener if needed
        """
        if not idict:
            idict = {}
        dict.__init__(self, idict)
        self.pycurl = idict.get('pycurl', True)
        self.capath = idict.get('capath', None)
        if self.pycurl:
            self.reqmgr = RequestHandler()

        # set up defaults
        self.setdefault("accept_type", 'text/html')
        self.setdefault("content_type", 'application/x-www-form-urlencoded')
        self.additionalHeaders = {}

        # check for basic auth early, as if found this changes the url
        urlComponent = sanitizeURL(url)
        if urlComponent['username'] is not None:
            self.addBasicAuth(urlComponent['username'], urlComponent['password'])
            # CouchDB 3.x requires user/passwd in the source/target of replication docs
            # More info in: https://github.com/dmwm/WMCore/pull/11001
            url = urlComponent['url']  # remove user, password from url

        self.setdefault("host", url)

        # then update with the incoming dict
        self.update(idict)

        self['endpoint_components'] = urlparse(self['host'])

        # If cachepath = None disable caching
        if 'cachepath' in idict and idict['cachepath'] is None:
            self["req_cache_path"] = None
        else:
            cache_dir = (self.cachePath(idict.get('cachepath'), idict.get('service_name')))
            self["cachepath"] = cache_dir
            self["req_cache_path"] = os.path.join(cache_dir, '.cache')
        self.setdefault("cert", None)
        self.setdefault("key", None)
        self.setdefault('capath', None)
        self.setdefault("timeout", 300)
        self.setdefault("logger", logging)

        check_server_url(self['host'])

    def get(self, uri=None, data={}, incoming_headers={},
            encode=True, decode=True, contentType=None):
        """
        GET some data
        """
        return self.makeRequest(uri, data, 'GET', incoming_headers,
                                encode, decode, contentType)

    def post(self, uri=None, data={}, incoming_headers={},
             encode=True, decode=True, contentType=None):
        """
        POST some data
        """
        return self.makeRequest(uri, data, 'POST', incoming_headers,
                                encode, decode, contentType)

    def put(self, uri=None, data={}, incoming_headers={},
            encode=True, decode=True, contentType=None):
        """
        PUT some data
        """
        return self.makeRequest(uri, data, 'PUT', incoming_headers,
                                encode, decode, contentType)

    def delete(self, uri=None, data={}, incoming_headers={},
               encode=True, decode=True, contentType=None):
        """
        DELETE some data
        """
        return self.makeRequest(uri, data, 'DELETE', incoming_headers,
                                encode, decode, contentType)

    def makeRequest(self, uri=None, data=None, verb='GET', incoming_headers=None,
                    encoder=True, decoder=True, contentType=None):
        """
        Wrapper around request helper functions.
        """
        data = data or {}
        incoming_headers = incoming_headers or {}
        data, headers = self.encodeParams(data, verb, incoming_headers, encoder, contentType)

        # both httpib2/pycurl require absolute url
        uri = self['host'] + uri
        if self.pycurl:
            result, response = self.makeRequest_pycurl(uri, data, verb, headers)
        else:
            result, response = self.makeRequest_httplib(uri, data, verb, headers)

        result = self.decodeResult(result, decoder)
        return result, response.status, response.reason, response.fromcache

    def makeRequest_pycurl(self, uri, data, verb, headers):
        """
        Make HTTP(s) request via pycurl library. Stay complaint with
        makeRequest_httplib method.
        """
        ckey, cert = self.getKeyCert()
        capath = self.getCAPath()

        headers["Accept-Encoding"] = "gzip,deflate,identity"

        response, result = self.reqmgr.request(uri, data, headers, verb=verb,
                                               ckey=ckey, cert=cert, capath=capath)
        return result, response

    def makeRequest_httplib(self, uri, data, verb, headers):
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
        if verb == 'GET' and data:
            uri = "%s?%s" % (uri, data)

        assert isinstance(data, (str, bytes)), \
            "Data in makeRequest is %s and not encoded to a string" % type(data)

        # And now overwrite any headers that have been passed into the call:
        # WARNING: doesn't work with deplate so only accept gzip
        headers["Accept-Encoding"] = "gzip,identity"

        # httplib2 will allow sockets to close on remote end without retrying
        # try to send request - if this fails try again - should then succeed
        try:
            conn = self._getURLOpener()
            response, result = conn.request(uri, method=verb, body=data, headers=headers)
            if response.status == 408:  # timeout can indicate a socket error
                raise socket.error
        except ServerNotFoundError as ex:
            # DNS cannot resolve this domain name, let's call it 'Service Unavailable'
            e = HTTPException()
            setattr(e, 'url', uri)
            setattr(e, 'status', 503)
            setattr(e, 'reason', 'Service Unavailable')
            setattr(e, 'result', str(ex))
            raise e from None
        except (socket.error, AttributeError):
            self['logger'].warn("Http request failed, retrying once again..")
            # AttributeError implies initial connection error - need to close
            # & retry. httplib2 doesn't clear httplib state before next request
            # if this is threaded this may spoil things
            # only have one endpoint so don't need to determine which to shut
            for con in viewvalues(conn.connections):
                con.close()
            conn = self._getURLOpener()
            # ... try again... if this fails propagate error to client
            try:
                response, result = conn.request(uri, method=verb, body=data, headers=headers)
            except AttributeError:
                msg = 'Error contacting: {}: {}'.format(self.getDomainName(), traceback.format_exc())
                # socket/httplib really screwed up - nuclear option
                conn.connections = {}
                raise socket.error(msg) from None
        if response.status >= 400:
            e = HTTPException()
            setattr(e, 'req_data', data)
            setattr(e, 'req_headers', headers)
            setattr(e, 'url', uri)
            setattr(e, 'result', result)
            setattr(e, 'status', response.status)
            setattr(e, 'reason', response.reason)
            setattr(e, 'headers', response)
            raise e

        return result, response

    def encodeParams(self, data, verb, incomingHeaders, encoder, contentType):
        """
        Encode request parameters for usage with the 4 verbs.
        Assume params is already encoded if it is a string and
        uses a different encoding depending on the HTTP verb
        (either json.dumps or urllib.urlencode)
        """
        # TODO: User agent should be:
        # $client/$client_version (CMS)
        # $http_lib/$http_lib_version $os/$os_version ($arch)
        headers = {"Content-type": contentType if contentType else self['content_type'],
                   "User-Agent": "WMCore.Services.Requests/v002",
                   "Accept": self['accept_type']}

        for key in self.additionalHeaders:
            headers[key] = self.additionalHeaders[key]
        # And now overwrite any headers that have been passed into the call:
        # WARNING: doesn't work with deplate so only accept gzip
        incomingHeaders["Accept-Encoding"] = "gzip,identity"
        headers.update(incomingHeaders)

        # If you're posting an attachment, the data might not be a dict
        #   please test against ConfigCache_t if you're unsure.
        # assert type(data) == type({}), \
        #        "makeRequest input data must be a dict (key/value pairs)"
        encoded_data = ''
        if verb != 'GET' and data:
            if isinstance(encoder, (types.MethodType, types.FunctionType)):
                encoded_data = encoder(data)
            elif encoder is False:
                # Don't encode the data more than we have to
                #  we don't want to URL encode the data blindly,
                #  that breaks POSTing attachments... ConfigCache_t
                # encoded_data = urllib.urlencode(data)
                #  -- Andrew Melo 25/7/09
                encoded_data = data
            else:
                # Either the encoder is set to True or it's junk, so use
                # self.encode
                encoded_data = self.encode(data)
            headers["Content-Length"] = len(encoded_data)
        elif verb != 'GET':
            # delete requests might not have any body
            headers["Content-Length"] = 0
        elif verb == 'GET' and data:
            # encode the data as a get string
            encoded_data = urlencode(data, doseq=True)

        return encoded_data, headers

    def decodeResult(self, result, decoder):
        """
        Decode the http/pycurl request result
        NOTE: if decoder is provided with a False value, then it means no
        decoding is applied on the results at all
        """
        if isinstance(decoder, (types.MethodType, types.FunctionType)):
            result = decoder(result)
        elif decoder is not False:
            result = self.decode(result)
        return result

    def encode(self, data):
        """
        encode data into some appropriate format, for now make it a string...
        """
        return urlencode(data, doseq=True)

    def decode(self, data):
        """
        decode data to some appropriate format, for now make it a string...
        """
        if PY3:
            return decodeBytesToUnicode(data)
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
            idir = tempfile.mkdtemp(prefix='.wmcore_cache_')
            # Alan Malta in 29 Mar 2022: this seems to prematurely remove the cache
            # directory. For details, see: https://github.com/dmwm/WMCore/pull/10915
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
            except Exception as ex:
                msg = 'No certificate or key found, authentication may fail'
                self['logger'].info(msg)
                self['logger'].debug(str(ex))

        try:
            # disable validation as we don't have a single PEM with all ca's
            http = httplib2.Http(self['req_cache_path'], self['timeout'],
                                 disable_ssl_certificate_validation=True)
        except TypeError:
            # old httplib2 versions disable validation by default
            http = httplib2.Http(self['req_cache_path'], self['timeout'])

        # Domain must be just a hostname and port. self[host] is a URL currently
        if key or cert:
            http.add_certificate(key=key, cert=cert, domain='')
        return http

    def addBasicAuth(self, username, password):
        """Add basic auth headers to request"""
        username = encodeUnicodeToBytes(username)
        password = encodeUnicodeToBytes(password)
        encodedauth = base64.encodebytes(b'%s:%s' % (username, password)).strip()
        if PY3:
            encodedauth = decodeBytesToUnicode(encodedauth)
        auth_string = "Basic %s" % encodedauth
        self.additionalHeaders["Authorization"] = auth_string

    def getKeyCert(self):
        """
       _getKeyCert_

       Get the user credentials if they exist, otherwise throw an exception.
       This code was modified from DBSAPI/dbsHttpService.py
        """

        # Zeroth case is if the class has over ridden the key/cert and has it
        # stored in self
        if self['cert'] and self['key']:
            key = self['key']
            cert = self['cert']
        else:
            key, cert = getKeyCertFromEnv()

        # Set but not found
        if key is None or cert is None:
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
        capath = self['capath']
        if not capath:
            capath = getCAPathFromEnv()
        return capath

    def uploadFile(self, fileName, url, fieldName='file1', params=[], verb='POST'):
        """
        Upload a file with curl streaming it directly from disk

        :rtype: bytes (both py2 and py3)
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
        bbuf = BytesIO()
        hbuf = BytesIO()
        c.setopt(pycurl.WRITEFUNCTION, bbuf.write)
        c.setopt(pycurl.HEADERFUNCTION, hbuf.write)
        if capath:
            c.setopt(pycurl.CAPATH, capath)
            c.setopt(pycurl.SSL_VERIFYPEER, True)
        else:
            c.setopt(pycurl.SSL_VERIFYPEER, False)
        if ckey:
            c.setopt(pycurl.SSLKEY, ckey)
        if cert:
            c.setopt(pycurl.SSLCERT, cert)
        c.perform()
        hres = hbuf.getvalue()
        bres = bbuf.getvalue()
        rh = ResponseHeader(hres)
        c.close()
        if rh.status < 200 or rh.status >= 300:
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

        hbuf = BytesIO()

        with open(fileName, "wb") as fp:
            curl = pycurl.Curl()
            curl.setopt(pycurl.URL, url)
            curl.setopt(pycurl.WRITEDATA, fp)
            curl.setopt(pycurl.HEADERFUNCTION, hbuf.write)
            if capath:
                curl.setopt(pycurl.CAPATH, capath)
                curl.setopt(pycurl.SSL_VERIFYPEER, True)
            else:
                curl.setopt(pycurl.SSL_VERIFYPEER, False)
            if ckey:
                curl.setopt(pycurl.SSLKEY, ckey)
            if cert:
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

    def __init__(self, url='http://localhost:8080', idict={}):
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
            if PY3:
                data = decodeBytesToUnicode(data)
            data = decoder.decode(data)
            unthunked = thunker.unthunk(data)
            return unthunked
        return {}


class TempDirectory(object):
    """
    Directory that cleans up after itself

    Except this doesn't work, python __del__ is NOT a destructor

    Leaving it anyways, since it might work sometimes

    """

    def __init__(self, idir):
        self.dir = idir

    def __del__(self):
        try:
            # it'll likely fail, but give it a try
            shutil.rmtree(self.dir, ignore_errors=True)
        except Exception:
            pass
