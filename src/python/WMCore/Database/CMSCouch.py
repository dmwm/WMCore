#!/usr/bin/env python
"""
_CMSCouch_

A simple API to CouchDB that sends HTTP requests to the REST interface.
"""

__revision__ = "$Id: CMSCouch.py,v 1.56 2010/04/22 15:58:00 metson Exp $"
__version__ = "$Revision: 1.56 $"

try:
    # Python 2.6
    import json
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json

import urllib
from httplib import BadStatusLine
import datetime
import threading
from WMCore.Services.Requests import BasicAuthJSONRequests

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
        """
        __to_json__

        This is here to prevent the serializer from attempting to serialize
        this object and adding a bunch of keys that couch won't understand.
        """
        jsonDict = {}
        for key in self.keys():
            jsonDict[key] = self[key]

        return jsonDict
    
class CouchDBRequests(BasicAuthJSONRequests):
    """
    CouchDB has two non-standard HTTP calls, implement them here for
    completeness, and talks to the CouchDB port
    """
    def __init__(self, url = 'localhost:5984'):
        BasicAuthJSONRequests.__init__(self, url)
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
            result, status, reason = BasicAuthJSONRequests.makeRequest(
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
            raise CouchBadRequestError( reason, data, result )
        elif (status == 403):
            raise CouchForbidden( reason, data, result )
        elif (status == 404):
            raise CouchNotFoundError( reason, data, result )
        elif (status == 405):
            raise CouchNotAllowedError( reason, data, result )
        elif (status == 409):
            raise CouchConflictError( reason, data, result )
        elif (status == 412):
            raise CouchPreconditionFailedError( reason, data, result )
        elif (status == 500):
            raise CouchInternalServerError( reason, data, result )
        elif (status >= 400):
            # we have a new error status, log it
            raise CouchError(reason, data, result, status)
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
        CouchDBRequests.__init__(self, url)
        self._reset_queue()
        self.name = urllib.quote_plus(dbname)
        self._queue_size = size
        self.threads = []
        self.last_seq = 0

    def _reset_queue(self):
        """
        Set the queue to an empty list, e.g. after a commit
        """
        self._queue = []

    def timestamp(self, data):
        """
        Time stamp each doc in a list 
        """
        if type(data) == type({}):
            data['timestamp'] = str(datetime.datetime.now())
        else:
            for doc in data:
                if 'timestamp' not in doc.keys():
                    doc['timestamp'] = str(datetime.datetime.now())
        return data

    def queue(self, doc, timestamp = False, viewlist=[]):
        """
        Queue up a doc for bulk insert. If timestamp = True add a timestamp
        field if one doesn't exist. Use this over commit(timestamp=True) if you
        want to timestamp when a document was added to the queue instead of when
        it was committed
        """
        if timestamp:
            self.timestamp(doc)
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
        uri  = '/%s/_bulk_docs/' % self.name
        if timestamp:
            self.timestamp(doc)
            
        data = {'docs': [doc]}
        retval = self.post(uri , data)
        return retval

    def commit(self, doc=None, returndocs = False, timestamp = False, viewlist=[]):
        """
        Add doc and/or the contents of self._queue to the database. If returndocs
        is true, return document objects representing what has been committed. If
        timestamp is true timestamp all documents with a date formatted like:
        2009/01/30 18:04:11 - this will be the timestamp of when the commit was
        called, it will not override an existing timestamp field.
        
        TODO: restore support for returndocs and viewlist
        
        Returns a list of good documents
            throws an exception otherwise
        """
        if (doc):
            self.queue(doc, timestamp, viewlist)
            
        if timestamp:
            self.timestamp(self._queue)
        # commit in thread to avoid blocking others
        uri  = '/%s/_bulk_docs/' % self.name
        
        data = {'docs': list(self._queue)}
        retval = self.post(uri , data)
        self._reset_queue()
        return retval

    def document(self, id):
        """
        Load a document identified by id
        """
        return Document(dict=self.get('/%s/%s' % (self.name, 
                                                  urllib.quote_plus(id))))

    def delete_doc(self, id):
        """
        Immediately delete a document identified by id
        """
        doc = self.document(id)
        doc.delete()
        self.commitOne(doc)

    def compact(self, views=[]):
        """
        Compact the database: http://wiki.apache.org/couchdb/Compaction
         
        If given, views should be a list of design document name (minus the 
        _design/ - e.g. myviews not _design/myviews). For each view in the list 
        view compaction will be triggered.
        """
        
        response = self.post('/%s/_compact' % self.name)
        if len(views) > 0:
            for view in views:
                response[view] = self.post('/%s/_compact/%s' % (self.name, view))
        return response
            
    def changes(self, since=-1):
        """
        Get the changes since sequence number. Store the last sequence value to
        self.last_seq. If the since is negative use self.last_seq. 
        """
        if since < 0:
            since = self.last_seq
        data = self.get('/%s/_changes/?since=%s' % (self.name, since))
        self.last_seq = data['last_seq']
        return data
        
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
        db = urllib.quote_plus(db)
        return self.delete("/%s" % db)

    def connectDatabase(self, db):
        if db not in self.listDatabases():
            self.createDatabase(db)
        db = urllib.quote_plus(db)
        return Database(db, self.url)

    def __str__(self):
        return self.listDatabases().__str__()
    
    def replicate(self, source, destination, continuous=False, create_target=False):
        #TODO: how to protect from missing http://?
        self.post('/_replicate', 
                  data={"source":source,
                        "target":destination, 
                        "continuous":continuous,
                        "create_target":create_target})

# define some standard couch error classes
# from:
#  http://wiki.apache.org/couchdb/HTTP_status_list

class CouchError(Exception):
    def __init__(self, reason, data, result, status = None):
        self.reason = reason
        self.data = data
        self.result = result
        self.type = "CouchError"
        self.status = status
    
    def __str__(self):
        if self.status != None:
            errorMsg = "NEW ERROR STATUS! UPDATE CMSCOUCH.PY!: %s\n" % self.status 
        else:
            errorMsg = ""
        return errorMsg + "%s - reason: %s, data: %s result: %s" % (self.type, 
                                             self.reason, 
                                             repr(self.data),
                                             self.result)
    
class CouchBadRequestError(CouchError):
    def __init__(self, reason, data, result):
        CouchError.__init__(reason, data, result)
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

class CouchForbidden(CouchError):
    def __init__(self, reason, data, result):
        CouchError.__init__(self, reason, data, result)
        self.type = "CouchForbidden"