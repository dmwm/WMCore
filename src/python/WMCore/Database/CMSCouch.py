#!/usr/bin/env python
"""
_CMSCouch_

A simple API to CouchDB that sends HTTP requests to the REST interface.

http://wiki.apache.org/couchdb/API_Cheatsheet
"""




import urllib
import datetime
import re

from httplib import HTTPException

from WMCore.Services.Requests import BasicAuthJSONRequests

def check_name(dbname):
    match = re.match("^[a-z0-9_$()+-/]+$", urllib.unquote_plus(dbname))
    if not match:
        msg = '%s is not a valid database name'
        raise ValueError(msg % urllib.unquote_plus(dbname))

def check_server_url(srvurl):
    good_name = srvurl.startswith('http://') or srvurl.startswith('https://')
    if not good_name:
        raise ValueError('You must include http(s):// in your servers address')
     
class Document(dict):
    """
    Document class is the instantiation of one document in the CouchDB
    """
    def __init__(self, id=None, dict = {}):
        """
        Initialise our Document object - a dictionary which has an id field
        """
        dict.__init__(self)
        self.update(dict)
        if id:
            self.setdefault("_id", id)

    def delete(self):
        """
        Mark the document as deleted
        """
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
    def __init__(self, url = 'http://localhost:5984'):
        """
        Initialise requests
        """
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
    
    def makeRequest(self, uri=None, data=None, type='GET', incoming_headers = {}, 
                     encode=True, decode=True, contentType=None, cache=False):
        """
        Make the request, handle any failed status, return just the data (for 
        compatibility). By default do not cache the response.
        
        TODO: set caching in the calling methods.
        """
        try:
            if not cache:
                incoming_headers.update({'Cache-Control':'no-cache'}) 
            result, status, reason, cached = BasicAuthJSONRequests.makeRequest(
                                        self, uri, data, type, incoming_headers,
                                        encode, decode,contentType)
        except HTTPException, e:
            self.checkForCouchError(getattr(e, "status", None),
                                    getattr(e, "reason", None), data)
            
        return result
    
    def checkForCouchError(self, status, reason, data = None, result = None):
        """
        _checkForCouchError_
        
        Check the HTTP status and raise an appropriate exception.
        """
        if status == 400:
            raise CouchBadRequestError(reason, data, result)
        elif status == 401:
            raise CouchUnauthorisedError(reason, data, result)
        elif status == 403:
            raise CouchForbidden(reason, data, result)
        elif status == 404:
            raise CouchNotFoundError(reason, data, result)
        elif status == 405:
            raise CouchNotAllowedError(reason, data, result)
        elif status == 409:
            raise CouchConflictError(reason, data, result)
        elif status == 412:
            raise CouchPreconditionFailedError(reason, data, result)
        elif status == 500:
            raise CouchInternalServerError(reason, data, result)
        else:
            # We have a new error status, log it
            raise CouchError(reason, data, result, status)

        return
        
class Database(CouchDBRequests):
    """
    Object representing a connection to a CouchDB Database instance.
    TODO: implement COPY and MOVE calls.
    TODO: remove leading whitespace when committing a view
    """
    def __init__(self, dbname = 'database', 
                  url = 'http://localhost:5984', size = 1000):
        """
        A set of queries against a CouchDB database
        """
        check_name(dbname)
            
        self.name = urllib.quote_plus(dbname)
         
        CouchDBRequests.__init__(self, url)
        self._reset_queue()
        
        self._queue_size = size
        self.threads = []
        self.last_seq = 0

    def _reset_queue(self):
        """
        Set the queue to an empty list, e.g. after a commit
        """
        self._queue = []

    def timestamp(self, data, label=''):
        """
        Time stamp each doc in a list 
        """
        if label == True:
            label = 'timestamp'
        
        if type(data) == type({}):
            data[label] = str(datetime.datetime.now())
        else:
            for doc in data:
                if label not in doc.keys():
                    doc[label] = str(datetime.datetime.now())
        return data

    def queue(self, doc, timestamp = False, viewlist=[]):
        """
        Queue up a doc for bulk insert. If timestamp = True add a timestamp
        field if one doesn't exist. Use this over commit(timestamp=True) if you
        want to timestamp when a document was added to the queue instead of when
        it was committed
        """
        if timestamp:
            self.timestamp(doc, timestamp)
        #TODO: Thread this off so that it's non blocking...
        if len(self._queue) >= self._queue_size:
            print 'queue larger than %s records, committing' % self._queue_size
            self.commit(viewlist=viewlist)
        self._queue.append(doc)

    def queueDelete(self, doc, viewlist=[]):
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
            self.timestamp(doc, timestamp)
            
        data = {'docs': [doc]}
        retval = self.post(uri , data)
        for v in viewlist:
            design, view = v.split('/')
            self.loadView(design, view, {'limit': 0})
        return retval

    def commit(self, doc=None, returndocs = False, timestamp = False, 
               viewlist=[]):
        """
        Add doc and/or the contents of self._queue to the database. If
        returndocs is true, return document objects representing what has been 
        committed. If timestamp is true timestamp all documents with a date 
        formatted like: 2009/01/30 18:04:11 - this will be the timestamp of when
        the commit was called, it will not override an existing timestamp field.
        if timestamp is a string that string will be used as the label for the
        timestamp.
        
        TODO: restore support for returndocs and viewlist
        
        Returns a list of good documents
            throws an exception otherwise
        """
        if (doc):
            self.queue(doc, timestamp, viewlist)
            
        if timestamp:
            self.timestamp(self._queue, timestamp)
        # commit in thread to avoid blocking others
        uri  = '/%s/_bulk_docs/' % self.name
        
        data = {'docs': list(self._queue)}
        retval = self.post(uri , data)
        self._reset_queue()
        for v in viewlist:
            design, view = v.split('/')
            self.loadView(design, view, {'limit': 0})
        return retval

    def document(self, id):
        """
        Load a document identified by id
        """
        return Document(dict=self.get('/%s/%s' % (self.name, 
                                                  urllib.quote_plus(id))))

    def documentExists(self, id):
        """
        Check if a document exists by ID
        """
        uri = "/%s/%s" % (self.name, urllib.quote_plus(id))
        docExists = False
        try:
            self.makeRequest(uri, {}, 'HEAD')
            return True
        except:
            return False
            
       

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
                response[view] = self.post('/%s/_compact/%s' % (self.name, 
                                                                view))
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
        encodedOptions = {}
        for k,v in options.iteritems():
            encodedOptions[k] = self.encode(v)
        
        if len(keys):
            if (encodedOptions):
                data = urllib.urlencode(encodedOptions)
                retval = self.post('/%s/_design/%s/_view/%s?%s' % \
                            (self.name, design, view, data), {'keys':keys})
            else:
                retval = self.post('/%s/_design/%s/_view/%s' % \
                            (self.name, design, view), {'keys':keys})
        else:
            retval = self.get('/%s/_design/%s/_view/%s' % \
                            (self.name, design, view), encodedOptions)
            
        if ('error' in retval):
            raise RuntimeError ,\
                    "Error in CouchDB: viewError '%s' reason '%s'" %\
                        (retval['error'], retval['reason'])
        else:
            return retval
        
    def loadList(self, design, list, view, options = {}, keys = []):
        """
        Load data from a list function. This returns data that hasn't been 
        decoded, since a list can return data in any format. It is expected that
        the caller of this function knows what data is being returned and how to
        deal with it appropriately.  
        """
        encodedOptions = {}
        for k,v in options.iteritems():
            encodedOptions[k] = self.encode(v)
        
        if len(keys):
            if (encodedOptions):
                data = urllib.urlencode(encodedOptions)
                retval = self.post('/%s/_design/%s/_list/%s/%s?%s' % \
                        (self.name, design, list, view, data), {'keys':keys}, 
                        decode=False)
            else:
                retval = self.post('/%s/_design/%s/_list/%s/%s' % \
                        (self.name, design, list, view), {'keys':keys}, 
                        decode=False)
        else:
            retval = self.get('/%s/_design/%s/_list/%s/%s' % \
                        (self.name, design, list, view), encodedOptions, 
                        decode=False)
            
        if ('error' in retval):
            raise RuntimeError ,\
                    "Error in CouchDB: viewError '%s' reason '%s'" %\
                        (retval['error'], retval['reason'])
        else:
            return retval
        
        
    def createDesignDoc(self, design='myview', language='javascript'):
        """
        Create a document that represents a design document
        """
        view = Document('_design/%s' % design)
        view['language'] = language
        view['views'] = {}
        return view

    def allDocs(self):
        """
        Return all the documents in the database
        """
        return self.get('/%s/_all_docs' % self.name)

    def info(self):
        """
        Return information about the databaes (size, number of documents etc).
        """
        return self.get('/%s/' % self.name)
    
    def addAttachment(self, id, rev, value, name=None):
        """
        Add an attachment to a document.
        """
        if (name == None):
            name = "attachment"
        return self.put('/%s/%s/%s?rev=%s' % (self.name, id, name, rev),
                         value,
                         encode = False)
    
    def getAttachment(self, id, name = "attachment"):
        """
        _getAttachment_
        
        Retrieve an attachment for a couch document.
        """
        url = "/%s/%s/%s" % (self.name, id, name)
        attachment = self.get(url, None, encode = False, decode = False)
        
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
    
    def __init__(self, dburl='http://localhost:5984'):
        """
        Set up a connection to the CouchDB server
        """
        check_server_url(dburl)
        CouchDBRequests.__init__(self, dburl)
        self.url = dburl

    def listDatabases(self):
        "List all the databases the server hosts"
        return self.get('/_all_dbs')

    def createDatabase(self, dbname):
        """
        A database must be named with all lowercase characters (a-z),
        digits (0-9), or any of the _$()+-/ characters and must end with a slash
        in the URL.
        """
        check_name(dbname)

        self.put("/%s" % urllib.quote_plus(dbname))
        # Pass the Database constructor the unquoted name - the constructor will 
        # quote it for us.
        return Database(dbname, self.url)

    def deleteDatabase(self, dbname):
        "Delete a database from the server"
        check_name(dbname)
        dbname = urllib.quote_plus(dbname)
        return self.delete("/%s" % dbname)

    def connectDatabase(self, dbname = 'database', create = True, size = 1000):
        """
        Return a Database instance, pointing to a database in the server. If the
        database doesn't exist create it if create is True.
        """ 
        check_name(dbname)
        if create and dbname not in self.listDatabases():
            return self.createDatabase(dbname)
        return Database(dbname, self.url, size)
    
    def replicate(self, source, destination, continuous = False, 
                  create_target = False, cancel = False, doc_ids=False,
                  filter = False, query_params = False):
        """Trigger replication between source and destination. Options are as
        described in http://wiki.apache.org/couchdb/Replication, in summary:
            continuous = bool, trigger continuous replication 
            create_target = bool, implicitly create the target database  
            cancel = bool, stop continuous replication
            doc_ids = list, id's of specific documents you want to replicate
            filter = string, name of the filter function you want to apply to 
                     the replication, the function should be defined in a design
                     document in the source database.  
            query_params = dictionary of parameters to pass into the filter 
                     function
                     
        Source and destination need to be appropriately urlquoted after the port
        number. E.g. if you have a database with /'s in the name you need to 
        convert them into %2F's. 
        
        TODO: Improve source/destination handling - can't simply URL quote, 
        though, would need to decompose the URL and rebuild it.
        """
        check_server_url(source)
        check_server_url(destination)
        data={"source":source,"target":destination}
        #There must be a nicer way to do this, but I've not had coffee yet...
        if continuous: data["continuous"] = continuous
        if create_target: data["create_target"] = create_target
        if cancel: data["cancel"] = cancel
        if doc_ids: data["doc_ids"] = doc_ids
        if filter:
            data["filter"] = filter
            if query_params:
                data["query_params"] = query_params
        self.post('/_replicate', data)
    
    def status(self):
        """
        See what active tasks are running on the server.
        """
        return {'databases': self.listDatabases(),
                'server_stats': self.get('/_stats'),
                'active_tasks': self.get('/_active_tasks')}
        
    def __str__(self):
        """
        List all the databases the server has
        """
        return self.listDatabases().__str__()

# define some standard couch error classes
# from:
#  http://wiki.apache.org/couchdb/HTTP_status_list

class CouchError(Exception):
    "An error thrown by CouchDB"
    def __init__(self, reason, data, result, status = None):
        Exception.__init__(self)
        self.reason = reason
        self.data = data
        self.result = result
        self.type = "CouchError"
        self.status = status
    
    def __str__(self):
        "Stringify the error"
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
        CouchError.__init__(self, reason, data, result)
        self.type = "CouchBadRequestError"
        
class CouchUnauthorisedError(CouchError):
    def __init__(self, reason, data, result):
        CouchError.__init__(self, reason, data, result)
        self.type = "CouchUnauthorisedError"
                
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
