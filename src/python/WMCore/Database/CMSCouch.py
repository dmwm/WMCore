#!/usr/bin/env python


"""
_CMSCouch_

A simple API to CouchDB that sends HTTP requests to the REST interface.
"""

__revision__ = "$Id: CMSCouch.py,v 1.29 2009/06/26 08:59:40 metson Exp $"
__version__ = "$Revision: 1.29 $"

try:
    # Python 2.6
    import json
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json
import urllib
from httplib import HTTPConnection
import httplib
import time
import datetime
import thread
import threading
import traceback
import types

def httpRequest(url, path, data, method='POST', viewlist=[]):
    """
    Make a request to the remote database. for a give URI. The type of
    request will determine the action take by the server (be careful with
    DELETE!). Data should usually be a dictionary of {dataname: datavalue}.
    """
    headers = {"Content-type": 'application/x-www-form-urlencoded', 
               "Accept": 'text/plain',
               'Connection':'Keep-Alive'}
    encoded_data = ''
    if method != 'GET' and data:
        if  type(data) is types.StringType:
            encoded_data = data
        else:
            encoded_data = json.dumps(data)
        headers["Content-length"] = len(encoded_data)
    else:
        #encode the data as a get string
        if  not data:
            data = {}
        path = "%s?%s" % (path, urllib.urlencode(data, doseq=True))
    conn = HTTPConnection(url)
#    httplib.HTTPConnecion.debuglevel = 1
    conn.request(method, path, encoded_data, headers)
    response = conn.getresponse()
    if  method == 'POST':
        data = {} # I don't need to read data on POST request
    else:
        data = response.read()
    conn.close()
    for view in viewlist:
        conn = HTTPConnection(url)
        conn.request('GET', "%s?limit=1" % view)
        res  = conn.getresponse()
#        view_data = res.read()
        conn.close()
    return response.status, data

class HttpRequestThread(threading.Thread):
    """
    TODO: Replace with a thread pool
    """
    def __init__(self, url, path, data, method):
        threading.Thread.__init__(self)
        self.url = url
        self.path = path
        self.data = data
        self.method = method
        self.retry = False

    def run(self):
        """
        Request data to/from couch. If necessary made a few retries.
        This method calls httpRequest and can be used in thread.
        """
        # TODO: think about failed request, how we can ensure
        # that all data will be injected properly
        status, data = httpRequest(self.url, self.path , self.data, self.method)
#        if  status - 400 >= 0 and not self.retry: 
            # trigger all cases with HTTP response 400 and above
            # try one more time
#            time.sleep(1)
#            self.retry = True
#            return self.run()

class Document(dict):
    """
    Document class is the instantiation of one document in the CouchDB
    """
    def __init__(self, id=None):
        dict.__init__(self)
        if id:
            self.setdefault("_id", id)

    def delete(self):
        self['_deleted'] = True
        
def makeDocument( data ):
    """
    helper function to wrap a plain dict (i.e. one returned by couchserver)
    in a Document instance
    
    We don't simply do a return Document( data ) because arguments to the constructor
    are stuck into the _id field, not added to the dict
    """
    document = Document()
    document.update( data )
    return document

    
class Requests:
    """
    Generic class for sending different types of HTTP Request to a given URL
    TODO: Find a better home for this than WMCore.Databases
    """

    def __init__(self, url = 'localhost'):
        self.accept_type = 'text/html'
        self.url = url
        self.conn = HTTPConnection(self.url)

    def get(self, uri=None, data=None, encoder = None, decoder=None):
        """
        Get a document of known id
        """
        return self.makeRequest(uri, data, 'GET', encoder, decoder)

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

    def makeRequest(self, uri=None, data=None, request='GET',
                     encode=None, decode=None):
        """
        Make a request to the remote database. for a give URI. The type of
        request will determine the action take by the server (be careful with
        DELETE!). Data should usually be a dictionary of {dataname: datavalue}.
        """
        # I didn't use encode parameter in httpRequest, instead it uses
        # data type to decide to encode it via json or not
        status, data = httpRequest(self.url, uri, data, request)
        if  status - 400 >= 0:
            print "HTTP request status", status
        if  (decode == False):
            return data
        else:
            return self.decode(data)

#        headers = {"Content-type": 
#                    'application/x-www-form-urlencoded', #self.accept_type,
#                    "Accept": self.accept_type}
#        encoded_data = ''
#            
#        if request != 'GET' and data:
#            if (encode == False):
#                encoded_data = data
#            else:
#                encoded_data = self.encode(data)
#            headers["Content-length"] = len(encoded_data)
#        else:
#            if  not data:
#                data = {}
#            uri = "%s?%s" % (uri, urllib.urlencode(data, doseq=True))
#        self.conn.connect()
#        self.conn.request(request, uri, encoded_data, headers)
#        response = self.conn.getresponse()

#        data = response.read()
#        self.conn.close()
#        if (decode == False):
#            return data
#        else:
#            return self.decode(data)

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
    TODO: remove leading whitespace when committing a view
    """
    def __init__(self, dbname = 'database', 
                  url = 'localhost:5984/', size = 1000):
        self._queue = []
        self.name = urllib.quote_plus(dbname)
        JSONRequests.__init__(self, url)
        self._queue_size = size
        self.threads = []

    def timestamp(self, data):
        """
        Time stamp each doc in a list - should really edit in place, something
        is up with the references...
        """
        if type(data) == type({}):
            data['timestamp'] = str(datetime.datetime.now())
            return data
        for doc in data:
            if 'timestamp' not in doc.keys():
                doc['timestamp'] = str(datetime.datetime.now())
        return list

    def queue(self, doc, timestamp = False, viewlist=[]):
        """
        Queue up a doc for bulk insert. If timestamp = True add a timestamp
        field if one doesn't exist. Use this over commit(timestamp=True) if you
        want to timestamp when a document was added to the queue instead of when
        it was committed
        """
        if timestamp:
            doc = self.timestamp(doc)
        #TODO: Thread this off so that it's non blocking...
        if len(self._queue) >= self._queue_size:
            print 'queue larger than %s records, committing' % self._queue_size
            self.commit(viewlist=viewlist)
        self._queue.append(doc)

    def queueDelete(self, doc):
        """
        Queue up a document for deletion
        """
        assert type(doc) == type({}), "document not a dictionary"
        doc['_deleted'] = True
        self.queue(doc)

    def commit(self, doc=None, returndocs = False, timestamp = False, viewlist=[]):
        """
        Add doc and/or the contents of self._queue to the database. If returndocs
        is true, return document objects representing what has been committed. If
        timestamp is true timestamp all documents with a date formatted like:
        2009/01/30 18:04:11 - this will be the timestamp of when the commit was
        called, it will not override an existing timestamp field.
        TODO: implement returndocs
        """
        result = ()
        if len(self._queue) > 0:
            if doc:
                self.queue(doc)
            if timestamp:
                self._queue = self.timestamp(self._queue)
            # commit in thread to avoid blocking others
            uri  = '/%s/_bulk_docs/' % self.name
            data = {'docs': list(self._queue)}
            status, data = httpRequest(self.url, uri , data, 'POST', viewlist)
#            thr  = HttpRequestThread(self.url, uri, data, 'POST')
#            thr.start() 
#            if  len(self._queue) < self._queue_size:
                # no more outstanding request, wait for all threads to finish
#                for ith in self.threads:
#                    ith.join()
#            else:
                # add thread to pool
#                self.threads.append(thr)

            # TODO: how to deal with threads, should we wait???
            # if we will wait for all request then we should use thr.join()
#            result = self.post('/%s/_bulk_docs/' % self.name, 
#                                 {'docs': self._queue})
            self._queue = []
            return result
        elif doc:
            if timestamp:
                doc = self.timestamp(doc)
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

    def loadView(self, design, view, options = {}, keys = []):
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
        for k,v in options.iteritems():
            options[k] = self.encode(v)
        # the following is CouchDB 090 only, this is the reference platform
        if len(keys):
            data = urllib.urlencode(options)
            return self.post('/%s/_design/%s/_view/%s?%s' % \
                            (self.name, design, view, data), {'keys':keys})
        else:
            return self.get('/%s/_design/%s/_view/%s' % \
                            (self.name, design, view), options)

    def createDesignDoc(self, design='myview', language='javascript'):
        view = Document('_design/%s' % design)
        view['language'] = language
        view['views'] = {}
        return view

    def allDocs(self):
        return self.get('/%s/_all_docs' % self.name)

    def info(self):
        return self.get('/%s/' % self.name)
    
    def addAttachment(self, id, rev, value, name=None):
        if (name == None):
            name = "attachment"
        return self.put('/%s/%s/%s?rev=%s' % (self.name, id, name, rev),
                         value,
                         False)
    
    def getAttachment(self, id, name=None):
        if (name == None):
            name = "attachment"
        attachment = self.get('/%s/%s/%s' % (self.name,id,name),
                         None,
                         False,
                         False)
        # there has to be a better way to do this but if we're not de-jsoning
        # the return values, then this is all I can do for error checking,
        # right?
        # TODO: MAKE BETTER ERROR HANDLING
        if (attachment.find('{"error":"not_found","reason":"deleted"}') != -1):
            raise RuntimeError, "File not found, deleted"
        return attachment
       
class CouchServer(CouchDBRequests):
    """
    An object representing the CouchDB server, use it to list, create, delete
    and connect to databases.

    More info http://wiki.apache.org/couchdb/HTTP_database_API
    """
    def listDatabases(self):
        return self.get('/_all_dbs')

    def createDatabase(self, db):
        """
        A database must be named with all lowercase characters (a-z),
        digits (0-9), or any of the _$()+-/ characters and must end with a slash
        in the URL - TODO assert this with a regexp
        """
        db = urllib.quote_plus(db)
        self.put("/%s" % db)
        return self.connectDatabase(db)

    def deleteDatabase(self, db):
        return self.delete("/%s" % db)

    def connectDatabase(self, db):
        return Database(db, self.url)

    def __str__(self):
        return self.listDatabases().__str__()
