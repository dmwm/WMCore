try:
    # Python 2.6
    import json
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json
import urllib
import os
from httplib import HTTPConnection

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
        self.setdefault("host", url)
        self.setdefault("conn", self._getURLOpener())

    def get(self, uri=None, data=None, encoder = None, decoder=None):
        """
        Get a document of known id
        """
        data = self.makeRequest(uri, data, 'GET', encoder, decoder)
        return data

    def post(self, uri=None, data=None, encoder = None, decoder=None):
        """
        POST some data
        """
        return self.makeRequest(uri, data, 'POST', encoder, decoder)

    def put(self, uri=None, data=None, encoder = None, decoder=None):
        """
        PUT some data
        """
        return self.makeRequest(uri, data, 'PUT', encoder, decoder)
       
    def delete(self, uri=None, data=None, encoder = None, decoder=None):
        """
        DELETE some data
        """
        return self.makeRequest(uri, data, 'DELETE', encoder, decoder)

    def makeRequest(self, uri=None, data=None, type='GET',
                     encode=True, decode=True):
        """
        Make a request to the remote database. for a give URI. The type of
        request will determine the action take by the server (be careful with
        DELETE!). Data should usually be a dictionary of {dataname: datavalue}.
        
        Returns a tuple of the data from the server, decoded using the 
        appropriate method the response status and the response reason, to be 
        used in error handling. 
        """
        headers = {"Content-type": 'application/x-www-form-urlencoded', 
                   "Accept": self['accept_type']}
        encoded_data = ''
        
        if type != 'GET' and data:
            if (encode == False):
                encoded_data = data
            else:
                encoded_data = self.encode(data)
            headers["Content-length"] = len(encoded_data)
        else:
            #encode the data as a get string
            if not data:
                data = {}
            uri = "%s?%s" % (uri, urllib.urlencode(data))
        self['conn'].connect()
        self['conn'].request(type, uri, encoded_data, headers)
        response = self['conn'].getresponse()
        data = response.read()
        self['conn'].close()
        
        if decode:
            data = self.decode(data)
        return data, response.status, response.reason

    def encode(self, data):
        """
        encode data into some appropriate format, for now make it a string...
        """
        return urllib.urlencode(data)

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

    def encode(self, data):
        """
        encode data as json
        """
        return json.dumps(data)

    def decode(self, data):
        """
        decode the data to python from json
        """
        if data:
            return json.loads(data)
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
        return HTTPSConnection(self['host'], key_file=cert, cert_file=key)
    
    def getKeyCert():
        """
       _getKeyCert_
       
       Gets the User Proxy if it exists, otherwise throws an exception.
       This code is borrowed from DBSAPI/dbsHttpService.py
        """
        # Zeroth case is if the class has over ridden the key/cert and has it
        # stored in self
        if self['cert'] and self['key']:
            key = self.key
            cert = self.cert
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
            msg = "Certificate is not found"
            raise Exception, msg
        
        # All looks OK, still doesn't gurantee proxy's validity etc.
        return key, cert