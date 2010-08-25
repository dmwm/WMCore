#!/usr/bin/env python


"""
_CMSCouch_

A simple API to CouchDB that sends HTTP requests to the REST interface.
"""

__revision__ = "$Id: CMSCouch.py,v 1.44 2009/08/11 15:53:34 meloam Exp $"
__version__ = "$Revision: 1.44 $"

try:
    # Python 2.6
    import json
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json
import urllib
from httplib import HTTPConnection, BadStatusLine
import time
import datetime
import thread
import threading
import traceback
import types
from WMCore.Services.Requests import JSONRequests

def httpRequest(url, path, data, method='POST', viewlist=[]):
    """
    Make a request to the remote database. for a given URI. The type of
    request will determine the action taken by the server (be careful with
    DELETE!). Data should usually be a dictionary of {dataname: datavalue}.
    """
    headers = {'Content-type': 'application/x-www-form-urlencoded',
                'Accept': 'text/plain'}
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
#    httplib.HTTPConnection.debuglevel = 1
    conn.request(method, path, encoded_data, headers)
    response = conn.getresponse()
    status = response.status
    data = response.read()
    conn.close()
    for view in viewlist:
        conn = HTTPConnection(url)
        conn.request('GET', "%s?limit=1" % view)
        res  = conn.getresponse()
        conn.close()
    return status, data

class HttpRequestThread(threading.Thread):
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
    def __init__(self, id=None, dict = {}):
        dict.__init__(self)
        self.update(dict)
        if id:
            self.setdefault("_id", id)

    def delete(self):
        self['_deleted'] = True
    
    def __to_json__(self, thunker):
        tmpDict = {}
        for k,v in self.iteritems():
            tmpDict[k] = v
        return tmpDict
    
    def __from_json__(self, input, thunker):
        if ('json_hack_mod_' in input):
            del input['json_hack_mod']
        if ('json_hack_name_' in input):
            del input['json_hack_name']
        return Document( dict = input )
    
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
    
    def makeRequest(self, uri=None, data=None, type='GET',
                     encode=True, decode=True, contentType=None):
        """
        Make the request, handle any failed status, return just the data (for 
        compatibility).
        """
        try:
            result, status, reason = JSONRequests.makeRequest(
                                        self, uri, data, type, encode, decode, contentType)
        except BadStatusLine,e:
            print "BadStatusLine failure: %s" % e
            print "     at uri: %s" % uri
            print "  with data: %s" % data
            raise
        self.checkForCouchError(status, reason, data, result)
        return result
    
    def checkForCouchError(self, status, reason, data = None, result = None):
        """
        Check the HTTP status and raise an appropriate exception 
        """
        if (status == 400 ):
            raise CouchBadRequestError( reason, result, data )
        elif (status == 404):
            raise CouchNotFoundError( reason, result,  data )
        elif (status == 405):
            raise CouchNotAllowedError( reason,  result, data )
        elif (status == 409):
            raise CouchConflictError( reason, result,  data )
        elif (status == 412):
            raise CouchPreconditionFailedError( reason, result,  data )
        elif (status == 500):
            raise CouchInternalServerError( reason, result,  data )
        elif (status >= 400):
            # we have a new error status, log it
            raise CouchError("""NEW ERROR STATUS! UPDATE CMSCOUCH.PY! 
            status: %s reason: %s data: %s result: %s""" % 
                            (status, reason, data, result))
        else:
            return
        
class Database(CouchDBRequests):
    """
    Object representing a connection to a CouchDB Database instance.
    TODO: implement COPY and MOVE calls.
    TODO: remove leading whitespace when committing a view
    """
    def __init__(self, dbname = 'database', 
                  url = 'localhost:5984', size = 1000):
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
        assert isinstance(doc, type({})), "document not a dictionary"
        doc['_deleted'] = True
        self.queue(doc)
    
    def commitOne(self, doc, returndocs=False, timestamp = False, viewlist=[]):
        """
            Helper function for when you know you only want to insert one doc
            additionally keeps from having to rewrite ConfigCache to handle the
            new commit function's semantics
        
        """
        tmpqueue = self._queue
        self._queue = []
        try:
            data = self.commit(doc,returndocs,timestamp,viewlist)
        except:
            self._queue = tmpqueue
            raise
#        finally:
#            self._queue = tmpqueue
        else:
            # if we made it out okay, put a flag there
            data[0][u'ok'] = True

            
        return data[0]
        
    def commit(self, doc=None, returndocs = False, timestamp = False, viewlist=[]):
        """
        Add doc and/or the contents of self._queue to the database. If returndocs
        is true, return document objects representing what has been committed. If
        timestamp is true timestamp all documents with a date formatted like:
        2009/01/30 18:04:11 - this will be the timestamp of when the commit was
        called, it will not override an existing timestamp field.
        
        Returns a list of good documents
            throws an exception otherwise
        """
        if (doc):
            self.queue(doc, timestamp, viewlist)
            
        if timestamp:
            self._queue = self.timestamp(self._queue)
        # commit in thread to avoid blocking others
        uri  = '/%s/_bulk_docs/' % self.name
        data = {'docs': list(self._queue)}
        retval = self.post(uri , data)
        self._queue = []
        return retval

#           ##############3
#           # currently nonfunctional threading code?
#
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
            if (options):
                data = urllib.urlencode(options)
                retval = self.post('/%s/_design/%s/_view/%s?%s' % \
                            (self.name, design, view, data), {'keys':keys})
            else:
                retval = self.post('/%s/_design/%s/_view/%s' % \
                            (self.name, design, view), {'keys':keys})
        else:
            retval = self.get('/%s/_design/%s/_view/%s' % \
                            (self.name, design, view), options)
            
        if ('error' in retval):
            raise RuntimeError ,\
                    "Error in CouchDB: viewError '%s' reason '%s'" %\
                        (retval['error'], retval['reason'])
        else:
            return retval
            
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
        if (id == "nonexistantid"):
            print attachment
        return attachment
       
class CouchServer(CouchDBRequests):
    """
    An object representing the CouchDB server, use it to list, create, delete
    and connect to databases.

    More info http://wiki.apache.org/couchdb/HTTP_database_API
    """
    
    def __init__(self, dburl='localhost:5984'):
        CouchDBRequests.__init__(self, dburl)
        self.url = dburl

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
        return Database(db, self.url)

    def deleteDatabase(self, db):
        return self.delete("/%s" % db)

    def connectDatabase(self, db):
        if db not in self.listDatabases():
            self.createDatabase(db)
        return Database(db, self.url)

    def __str__(self):
        return self.listDatabases().__str__()


# define some standard couch error classes
# from:
#  http://wiki.apache.org/couchdb/HTTP_status_list

class CouchError(Exception):
    def __init__(self, reason, data, result):
        self.reason = reason
        self.data = data
        self.result = result
        self.type = "CouchError"
    
    def __str__(self):
        return "%s - reason: %s, data: %s result: %s" % (self.type, 
                                             self.reason, 
                                             repr(self.data),
                                             self.result)
    
class CouchBadRequestError(CouchError):
    def __init__(self, reason, data):
        CouchError.__init__(reason, data)
        self.type = "CouchBadRequestError"
                
class CouchNotFoundError(CouchError):
    def __init__(self, reason, data, result):
        CouchError.__init__(self, reason, data, result)
        self.type = "CouchNotFoundError"
        
class CouchNotAllowedError(CouchError):
    def __init__(self, reason, data, result):
        CouchError.__init__(self, reason, data, result)
        self.type = "CouchNotAllowedError"
        
class CouchConflictError(CouchError):
    def __init__(self, reason, data, result):
        CouchError.__init__(self, reason, data, result)
        self.type = "CouchConflictError"
        
class CouchPreconditionFailedError(CouchError):
    def __init__(self, reason, data, result):
        CouchError.__init__(self, reason, data, result)
        self.type = "CouchPreconditionFailedError"
        
class CouchInternalServerError(CouchError):
    def __init__(self, reason, data, result):
        CouchError.__init__(self, reason, data, result)
        self.type = "CouchInternalServerError"
