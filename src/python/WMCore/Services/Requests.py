try:
    # Python 2.6
    import json
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json
import urllib
import os
import sys
from httplib import HTTPConnection
from httplib import HTTPSConnection
from sets import Set
from WMCore.WMException import WMException
import types
import pprint

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
            
        headers["Content-length"] = len(encoded_data)
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
        return HTTPConnection(self['host'], 443)

class _EmptyClass:
    pass


class JSONThunker:
    """
    _JSONThunker_
    Converts an arbitrary object to <-> from a jsonable object.
    
    Will, for the most part "do the right thing" about various instance objects
    by storing their class information along with their data in a dict. Handles
    a recursion limit to prevent infinite recursion.
    
    self.passThroughTypes - stores a list of types that should be passed
      through unchanged to the JSON parser
      
    self.blackListedModules - a list of modules that should not be stored in
      the JSON.
    
    """
    def __init__(self):
        self.passThroughTypes = (types.NoneType,
                                 types.BooleanType,
                                 types.IntType,
                                 types.LongType,
                                 types.ComplexType,
                                 types.StringTypes,
                                 types.StringType,
                                 types.UnicodeType
                                 )
        # objects that inherit from dict should be treated as a dict
        #   they don't store their data in __dict__. There was enough
        #   of those classes that it warrented making a special case
        self.dictSortOfObjects = ( ('WMCore.Datastructs.Job', 'Job'),
                                   ('WMCore.WMBS.Job', 'Job'),
                                   ('WMCore.Database.CMSCouch', 'Document' ))
        # ditto above, but for lists
        self.listSortOfObjects = ( ('WMCore.DataStructs.JobPackage', 'JobPackage' ),
                                   ('WMCore.WMBS.JobPackage', 'JobPackage' ),)
        
        self.foundIDs = {}
        # modules we don't want JSONed
        self.blackListedModules = ('sqlalchemy.engine.threadlocal',
                                   'WMCore.Database.DBCore',
                                   'logging',
                                   'WMCore.DAOFactory',
                                   'WMCore.WMFactory',
                                   'WMFactory',
                                   'WMCore.Configuration',
                                   'WMCore.Database.Transaction',
                                   'threading',
                                   'datetime')
        
    def checkRecursion(self, data):
        """
        handles checking for infinite recursion
        """
        if (id(data) in self.foundIDs):
            if (self.foundIDs[id(data)] > 5):
                self.unrecurse(data)
                return "**RECURSION**"
            else:
                self.foundIDs[id(data)] += 1
                return data
        else:
            self.foundIDs[id(data)] = 1
            return data
           
    def unrecurse(self, data):
        """
        backs off the recursion counter if we're returning from _thunk
        """
        self.foundIDs[id(data)] = self.foundIDs[id(data)] -1
         
    def checkBlackListed(self, data):
        """
        checks to see if a given object is from a blacklisted module
        """
        try:
            # special case
            if ((data.__class__.__module__ == 'WMCore.Database.CMSCouch') and
                (data.__class__.__name__ == 'Document')):
                data.__class__ = type({})
                return data
            if (data.__class__.__module__ in self.blackListedModules):
                return "Blacklisted JSON object: module %s, name %s, str() %s" %\
                    (data.__class__.__module__,data.__class__.__name__ , str(data))
            else:
                return data
        except:
            return data

    
    def thunk(self, toThunk):
        """
        Thunk - turns an arbitrary object into a JSONable object
        """
#        print "entering thunk"
#        pp = pprint.PrettyPrinter(indent = 4)
#        pp.pprint(toThunk)
        self.foundIDs = {}
        data = self._thunk(toThunk)
#        print "leaving thunk"
#        pp.pprint(data)
        return data
    
    def _thunk(self, toThunk):
        """
        helper function for thunk, does the actual work
        """
        toThunk = self.checkRecursion( toThunk )
        if (type(toThunk) in self.passThroughTypes):
            self.unrecurse(toThunk)
            return toThunk
        elif (type(toThunk) == type([])):
            for k,v in enumerate(toThunk):
                toThunk[k] = self._thunk(v)
            self.unrecurse(toThunk)
            return toThunk
        
        elif (type(toThunk) == type({})):
            for k,v in toThunk.iteritems():
                toThunk[k] = self._thunk(v)
            self.unrecurse(toThunk)
            return toThunk
        
        elif ((type(toThunk) == type(Set()))):
            tempDict = {'hack_to_encode_a_set_in_json_':True}
            counter = 0
            for val in toThunk:
                tempDict[counter] = self._thunk(val)
                counter = counter + 1
            self.unrecurse(toThunk)
            return tempDict
        elif (type(toThunk) == types.FunctionType):
            self.unrecurse(toThunk)
            return "function reference"
        elif (isinstance(toThunk, object)):
            toThunk = self.checkBlackListed(toThunk)
            
            if (type(toThunk) == type("")):
                # things that got blacklisted
                return toThunk
            if (hasattr(toThunk, '__to_json__')):
                print "tojson on %s " % toThunk.__class__.__module___
                toThunk2 = toThunk.__to_json__(self)
                self.unrecurse(toThunk)
                return toThunk2
            elif ( (toThunk.__class__.__module__, toThunk.__class__.__name__) in
                   self.dictSortOfObjects ):
                toThunk2 = self.handleDictObjectThunk( toThunk )
                self.unrecurse(toThunk)
                return toThunk2
            elif ( (toThunk.__class__.__module__, toThunk.__class__.__name__) in
                   self.listSortOfObjects ):
                toThunk2 = self.handleListObjectThunk( toThunk )
                self.unrecurse(toThunk)
                return toThunk2
            else:
                try:
                    tempDict = {'json_hack_mod_' : toThunk.__class__.__module__,
                                'json_hack_name_': toThunk.__class__.__name__, }
                    for idx in toThunk.__dict__:
                        tempDict[idx] = self._thunk(toThunk.__dict__[idx])
                    self.unrecurse(toThunk)
                    return tempDict
                except Exception, e:
                    tempDict = {'json_thunk_exception_' : "%s" % e }
                    self.unrecurse(toThunk)
                    return tempDict
        else:
            self.unrecurse(toThunk)
            raise RuntimeError, type(toThunk)
    
    def handleDictObjectThunk(self, data):
        tempDict = {'json_hack_mod_' : data.__class__.__module__,
                                'json_hack_name_': data.__class__.__name__, }
        for k,v in data.iteritems():
            tempDict[k] = self._thunk(v)            
        return tempDict
    
    def handleDictObjectUnThunk(self, value, data):
        for k,v in data.iteritems():
            value[k] = self._unthunk(v)
        return value
    
    def handleListObjectThunk(self, data):
        tempDict = {'json_hack_mod_' : data.__class__.__module__,
                                'json_hack_name_': data.__class__.__name__, 'json_list_data_' : [] }
        for k,v in enumerate(data):
            tempDict['json_list_data_'].append(self._thunk(v))            
        return tempDict
    
    def handleListObjectUnThunk(self, value, data):
        for k,v in enumerate(data['json_list_data_']):
            data['json_list_data_'][k] = self._unthunk(v)
        value.extend(data['json_list_data_'])
        return value
    
    def unthunk(self, data):
        """
        unthunk - turns a previously 'thunked' object back into a python object
        """
#        print "entering unthunk"
#        pp = pprint.PrettyPrinter(indent = 4)
#        pp.pprint(data)
        return self._unthunk(data)
    
    def _unthunk(self, data):
        """
        _unthunk - does the actual work for unthunk
        """
        if (type(data) == types.UnicodeType):
            return str(data)
        if (type(data) == type({})):
            if ('hack_to_encode_a_set_in_json_' in data):
                del data['hack_to_encode_a_set_in_json_']
                newSet = Set()
                for k,v in data.iteritems():
                    newSet.add( self._unthunk(v) )
                return newSet
            elif ( ('json_hack_mod_' in data) and ('json_hack_name_' in data) ):
                # spawn up an instance.. good luck
                #   here be monsters
                #   inspired from python's pickle code
                module = data['json_hack_mod_']
                name   = data['json_hack_name_']
                __import__(module)
                mod = sys.modules[module]
                try:
                    ourClass = getattr(mod, name)
                except:
                    print "failed to get %s from %s" % (mod, name)
                    raise
                if (module == 'WMCore.Services.Requests') and (name == JSONThunker):
                    raise RuntimeError, "Attempted to unthunk a JSONThunker.."
                value = _EmptyClass()
                del data['json_hack_mod_']
                del data['json_hack_name_']
                if (hasattr(ourClass, '__from_json__')):
                    try:
                        value.__class__ = ourClass
                    except:
                        value = ourClass()
                    value = ourClass.__from_json__(value, data, self)
                elif ( (module, name) in self.dictSortOfObjects ):  
                    try:
                        value.__class__ = ourClass
                    except:
                        value = ourClass()
                    value = self.handleDictObjectUnThunk( value, data )
                elif ( (module, name) in self.listSortOfObjects ):  
                    try:
                        value.__class__ = ourClass
                    except:
                        value = ourClass()
                    value = self.handleListObjectUnThunk( value, data )
                else:
                    if (type(ourClass) == types.ClassType):
                        value.__class__ = ourClass
                        value.__dict__ = data
                    else:
                        value = ourClass()
                        value.__dict__ = data
                
                return value
            else:
                for k,v in data.iteritems():
                    data[k] = self._unthunk(v)
                return data
 
        else:
            return data


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
        encoder = json.JSONEncoder()
        thunker = JSONThunker()
        thunked = thunker.thunk(data)
        return encoder.encode(thunked)
    

    def decode(self, data):
        """
        decode the data to python from json
        """
        if data:
            thunker = JSONThunker()
            decoder = json.JSONDecoder()
            data =  decoder.decode(data)
            thunked = thunker.unthunk(data)
            return thunked
        else:
            return {}      
        
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
        elif os.environ.has_key('X509_USER_PROXY'):
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
