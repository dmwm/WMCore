#!/usr/bin/python
"""
_Requests_

A set of classes to handle making http and https requests to a remote server and
deserialising the response.
"""

__revision__ = "$Id: Requests.py,v 1.35 2010/02/26 11:42:58 metson Exp $"
__version__ = "$Revision: 1.35 $"

import urllib
from urlparse import urlunparse
import os
import base64
from httplib import HTTPConnection
from httplib import HTTPSConnection
from httplib import HTTPException
from WMCore.WMException import WMException
from WMCore.Wrappers import JsonWrapper as json
from WMCore.Wrappers.JsonWrapper import JSONEncoder, JSONDecoder
from WMCore.Wrappers.JsonWrapper.JSONThunker import JSONThunker

class Requests(dict):
    """
    Generic class for sending different types of HTTP Request to a given URL
    """

    def __init__(self, url = 'localhost', dict={}):
        """
        url should really be host - TODO fix that when have sufficient code 
        coverage
        """
        #set up defaults
        self.setdefault("accept_type", 'text/html')
        self.setdefault("content_type", 'application/x-www-form-urlencoded')
        self.setdefault("host", url)
        
        # then update with the incoming dict
        self.update(dict)
        
        # and then get the URL opener
        self.setdefault("conn", self._getURLOpener())
        self.additionalHeaders = {}
        return

    def get(self, uri=None, data={}, encode = True, decode=True, contentType=None):
        """
        GET some data
        """
        return self.makeRequest(uri, data, 'GET', encode, decode, contentType)

    def post(self, uri=None, data={}, encode = True, decode=True, contentType=None):
        """
        POST some data
        """
        return self.makeRequest(uri, data, 'POST', encode, decode, contentType)

    def put(self, uri=None, data={}, encode = True, decode=True, contentType=None):
        """
        PUT some data
        """
        return self.makeRequest(uri, data, 'PUT', encode, decode, contentType)
       
    def delete(self, uri=None, data={}, encode = True, decode=True, contentType=None):
        """
        DELETE some data
        """
        return self.makeRequest(uri, data, 'DELETE', encode, decode, contentType)

    def makeRequest(self, uri=None, data={}, verb='GET',
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
            
        headers["Content-length"] = len(encoded_data)
        self['conn'].connect()
        assert type(encoded_data) == type('string'), \
                    "Data in makeRequest is %s and not encoded to a string" % type(encoded_data)
        
        self['conn'].request(verb, uri, encoded_data, headers)
        response = self['conn'].getresponse()
        result = response.read()
        self['conn'].close()
        if response.status >= 400:
            e = HTTPException()
            setattr(e, 'req_data', encoded_data)
            setattr(e, 'req_headers', headers)
            setattr(e, 'url', self.buildURL(uri)) 
            setattr(e, 'result', result)
            setattr(e, 'status', response.status)
            setattr(e, 'reason', response.reason)
            setattr(e, 'headers', response.getheaders())
            raise e
              
        if type(decoder) == type(self.makeRequest) or type(decoder) == type(f):
            result = decoder(result)
        elif decoder != False:
            result = self.decode(result)
        
        return result, response.status, response.reason
    
    def buildURL(self, uri):
        scheme = 'http'
        if self['conn'].__class__.__name__.startswith('HTTPS'):
            scheme = 'https'
        netloc = '%s:%s' % (self['conn'].host, self['conn'].port)
        return urlunparse([scheme, netloc, uri, '', '', ''])
         
    
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
        return HTTPConnection(self['host'])

                   
class JSONRequests(Requests):
    """
    Example implementation of Requests that encodes data to/from JSON.
    """
    def __init__(self, url = 'localhost:8080'):
        Requests.__init__(self, url)
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
    def __init__(self, url = "localhost:8080"):
        if url.find("@") == -1:
            JSONRequests.__init__(self, url)
            return

        (auth, hostname) = url.split("@", 2)

        JSONRequests.__init__(self, hostname)
        self.additionalHeaders["Authorization"] = \
            "Basic " + base64.encodestring(auth).strip()

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
        return HTTPSConnection(self['host'])

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
        return HTTPSConnection(self["host"])    
    
class SecureRequests(Requests):
    """
    Implementation of Requests using a different connection type, e.g. use HTTPS
    to send requests to a given URL, authenticating via a key/cert pair
    """ 
    def _getURLOpener(self):
        """
        method getting a secure (HTTPS) connection
        """
        key, cert = self.getKeyCert()
        return HTTPSConnection(self['host'], key_file=key, cert_file=cert)
    
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
