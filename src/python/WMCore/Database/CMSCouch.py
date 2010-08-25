#!/usr/bin/env python


"""
_CMSCouch_

A simple API to CouchDB that sends HTTP requests to the REST interface.
"""

__revision__ = "$Id: CMSCouch.py,v 1.13 2009/04/20 16:07:39 metson Exp $"
__version__ = "$Revision: 1.13 $"

try:
    # Python 2.6
    import json
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json
import urllib
from httplib import HTTPConnection
import uuid

class Document(dict):
    def __init__(self, id=None):
        dict.__init__(self)
        if id:
            self.setdefault("_id", id)
        else:
            self.setdefault("_id", uuid.uuid1())
    
    def delete(self):
        self['_deleted'] = True
        
class Requests:
    """
    Generic class for sending different types of HTTP Request to a given URL
    TODO: Find a better home for this than WMCore.Databases
    """ 
    
    def __init__(self, url = 'localhost'):
        self.accept_type = 'text/html'
        self.url = url
        self.conn = HTTPConnection(self.url)
        
    def get(self, uri=None, data=None):
        """
        Get a document of known id
        TODO: take some data
        """
        return self.makeRequest(uri, data)
        
    def post(self, uri=None, data=None):
        """
        POST some data
        """
        return self.makeRequest(uri, data, 'POST')
    
    def put(self, uri=None, data=None):
        """
        PUT some data
        """
        return self.makeRequest(uri, data, 'PUT')
        
    def delete(self, uri=None, data=None):
        """
        DELETE some data
        """
        return self.makeRequest(uri, data, 'DELETE')
        
    def makeRequest(self, uri=None, data=None, type='GET'):
        """
        Make a request to the remote database. for a give URI. The type of 
        request will determine the action take by the server (be careful with 
        DELETE!). Data should usually be a dictionary of {dataname: datavalue}.
        """
        headers = {"Content-type": 'application/x-www-form-urlencoded', #self.accept_type, 
                    "Accept": self.accept_type}
        encoded_data = ''
        if type != 'GET' and data:
            encoded_data = self.encode(data)
            headers["Content-length"] = len(encoded_data)
        else:
            #encode the data as a get string
            if  not data:
                data = {}
            uri = "%s?%s" % (uri, urllib.urlencode(data))
        self.conn.connect()
        self.conn.request(type, uri, encoded_data, headers)
        response = self.conn.getresponse()
        
        data = response.read()
        self.conn.close()
        return self.decode(data)
    
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

class JSONRequests(Requests): 
    """
    Implementation of Requests that encodes data to JSON.
    """
    def __init__(self, url = 'localhost:8080'):
        Requests.__init__(self, url)
        self.accept_type = "application/json"
        
    def encode(self, data):
        """
        encode data as json
        """
        return json.dumps(data)
    
    def decode(self, data):
        """
        decode the data to python from json
        """ 
        return json.loads(data)

class CouchDBRequests(JSONRequests):
    """
    CouchDB has two non-standard HTTP calls, implement them here for 
    completeness, and talks to the CouchDB port
    """
    def __init__(self, url = 'localhost:5984'):
        JSONRequests.__init__(self, url)
        self.accept_type = "application/json"
    def move(self, uri=None, data=None):
        """
        MOVE some data
        """
        return self.makeRequest(uri, data, 'MOVE')

    def copy(self, uri=None, data=None):
        """
        COPY some data
        """
        return self.makeRequest(uri, data, 'COPY')
     
class Database(CouchDBRequests):
    """
    Object representing a connection to a CouchDB Database instance.
    TODO: implement COPY and MOVE calls.
    """
    def __init__(self, dbname = 'database', url = 'http://localhost:5984/'):
        self._queue = []
        self.name = dbname
        JSONRequests.__init__(self, url)
        
    def queue(self, doc):
        """
        Queue up a doc for bulk insert
        """
        self._queue.append(doc)
        
    def queuedelete(self, doc):
        """
        Queue up a document for deletion
        """
        assert type(doc) == type({}), "document not a dictionary"
        doc['_deleted'] = True
        self.queue(doc)
        
    def commit(self, doc=None, returndocs = False):
        """
        Add doc and/or the contents of self._queue to the database. If returndocs
        is true, return document objects representing what has been committed.
        """
        result = ()
        if len(self._queue) > 0:
            if doc:
                self.queue(doc)
            result = self.post('/%s/_bulk_docs/' % self.name, {'docs': self._queue})
            self._queue = []
            return result
        elif doc:
            if  '_id' in doc.keys():
                return self.put('/%s/%s' % (self.name, 
                                            urllib.quote_plus(doc['_id'])), 
                                            doc)
            else:
                return self.post('/%s' % self.name, doc)
    
    def document(self, id):
        """
        Load a document identified by id
        """
        return self.get('/%s/%s' % (self.name, urllib.quote_plus(id)))
    
    def compact(self):
        """
        Compact the database: http://wiki.apache.org/couchdb/Compaction
        """
        return self.post('/%s/_compact' % self.name)
        
    def loadview(self, design, view, options = {}, keys = []):
        """
        Load a view by getting, for example:
        http://localhost:5984/tester/_view/viewtest/age_name?count=10&group=true
        
        The following URL query arguments are allowed:

        GET
                key=keyvalue
                startkey=keyvalue
                startkey_docid=docid
                endkey=keyvalue
                endkey_docid=docid
                limit=max rows to return 
                stale=ok
                descending=true
                skip=number of rows to skip
                group=true Version 0.8.0 and forward
                group_level=int
                reduce=false Trunk only (0.9)
                include_docs=true Trunk only (0.9)
        POST
                {"keys": ["key1", "key2", ...]} Trunk only (0.9)
                
        more info: http://wiki.apache.org/couchdb/HTTP_view_API
        """
        
        # the following is CouchDB 090 only, this is the reference platform
        if len(keys):
            data = urllib.urlencode(options)
            return self.post('/%s/_design/%s/_view/%s?%s' % \
                            (self.name, design, view, data), {'keys':keys})
        else:
            return self.get('/%s/_design/%s/_view/%s' % \
                            (self.name, design, view), options)
        
    def allDocs(self):
        return self.get('/%s/_all_docs' % self.name)
    
    def info(self):
        return self.get('/%s/' % self.name)
    
class CouchServer(CouchDBRequests):
    """
    An object representing the CouchDB server, use it to list, create, delete 
    and connect to databases. 
    """    
    def listDatabases(self):
        return self.get('/_all_dbs')
    
    def createDatabase(self, db):
        self.put("/%s" % db)
        return self.connectDatabase(db)
    
    def deleteDatabase(self, db):
        return self.delete("/%s" % db)
    
    def connectDatabase(self, db):
        return Database(db, self.url)
    
    def __str__(self):
        return self.listDatabases().__str__()
