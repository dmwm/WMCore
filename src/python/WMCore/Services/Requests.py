try:
    # Python 2.6
    import json
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json
import urllib
import os
from httplib import HTTPConnection
from httplib import HTTPSConnection
from sets import Set
from WMCore.WMException import WMException
import types

class Requests(dict):
    """
    Generic class for sending different types of HTTP Request to a given URL
    """

    def __init__(self, url = 'localhost'):
        """
        url should really be host - TODO fix that when have sufficient code 
        coverage
        """
        self.setdefault("accept_type", 'text/html')
        self.setdefault("content_type", 'application/x-www-form-urlencoded')
        self.setdefault("host", url)
        self.setdefault("conn", self._getURLOpener())

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
            uri = "%s?%s" % (uri, urllib.urlencode(data))
            
        self['conn'].connect()
        assert type(encoded_data) == type('string'), \
                    "Data in makeRequest is %s and not encoded to a string" % type(encoded_data)
        
        self['conn'].request(verb, uri, encoded_data, headers)
        response = self['conn'].getresponse()
        data = response.read()
        self['conn'].close()
        
        if type(decoder) == type(self.makeRequest) or type(decoder) == type(f):
            data = decoder(data)
        elif decoder != False:
            data = self.decode(data)
        return data, response.status, response.reason

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

class JSONSetEncoder(json.JSONEncoder):
    """
    Subclass of the json stuff to handle sets
    """
    def default(self, toEncode):
        if (type(toEncode) == type(Set())):
            tempDict = {'_hack_to_encode_a_set_in_json':True}
            counter = 0
            for item in toEncode:
                tempDict[counter] = item
                counter += 1
            return tempDict
        elif (isinstance(toEncode, object)):
            ourdict = toEncode.__dict__
            ourdict['_json_hack_type'] = "%s" % toEncode.__class__
            return ourdict
        else:
            return "**PLACEHOLDER** NEED TO FIX"
                
def JSONDecodeSetCallback(toDecode):
    if '_hack_to_encode_a_set_in_json' in toDecode:
        del toDecode['_hack_to_encode_a_set_in_json']
        try:
            return Set(toDecode.values())
        except:
            return "setfail in requests.py"
    else:
        return toDecode


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
        encoder = JSONSetEncoder()
        return encoder.encode(data)
    

    def decode(self, data):
        """
        decode the data to python from json
        """
        if data:
            decoder = json.JSONDecoder(object_hook = JSONDecodeSetCallback)
            return decoder.decode(data)
        else:
            return {}      
        
class SecureRequests(Requests):
    """
    Example implementation of Requests using a different connection type, e.g. 
    use HTTPS to send requests to a given URL, authenticating via a key/cert 
    pair
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
        elif os.environ.has_key('X509_USER_PROXY'):
            cert = os.environ['X509_USER_PROXY']
            key = cert
    
        # Third preference to User Cert/Proxy combinition
        elif os.environ.has_key('X509_USER_CERT'):
            cert = os.environ['X509_USER_CERT']
            key = os.environ['X509_USER_KEY']
        
        #TODO: only in linux, unix case, add other os case
        # Worst case, look for proxy at default location /tmp/x509up_u$uid
        else :
            uid = os.getuid()
            cert = '/tmp/x509up_u'+str(uid)
            key = cert
    
        #Set but not found
        if not os.path.exists(cert) or not os.path.exists(key):
            raise WMException('Request requires a host certificate and key', 
                              "WMCORE-11")
            
        # All looks OK, still doesn't gurantee proxy's validity etc.
        return key, cert
