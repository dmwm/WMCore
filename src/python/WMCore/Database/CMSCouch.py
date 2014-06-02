#!/usr/bin/env python
"""
_CMSCouch_

A simple API to CouchDB that sends HTTP requests to the REST interface.

http://wiki.apache.org/couchdb/API_Cheatsheet

NOT A THREAD SAFE CLASS.
"""



import time
import urllib
import re
import hashlib
import base64
import logging
from httplib import HTTPException
from datetime import timedelta, datetime

from WMCore.Services.Requests import JSONRequests
from WMCore.Lexicon import replaceToSantizeURL

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
    def __init__(self, id=None, inputDict = {}):
        """
        Initialise our Document object - a dictionary which has an id field
        inputDict - input dictionary to initialise this instance
        """
        dict.__init__(self)
        self.update(inputDict)
        if id:
            self.setdefault("_id", id)

    def delete(self):
        """
        Mark the document as deleted
        """
        # https://issues.apache.org/jira/browse/COUCHDB-1141
        deletedDict = { '_id' : self['_id'], '_rev' : self['_rev'], '_deleted' : True }
        self.update(deletedDict)
        for key in self.keys():
            if key not in deletedDict:
                del self[key]

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

class CouchDBRequests(JSONRequests):
    """
    CouchDB has two non-standard HTTP calls, implement them here for
    completeness, and talks to the CouchDB port
    """
    def __init__(self, url = 'http://localhost:5984', usePYCurl = False, ckey = None, cert = None, capath = None):
        """
        Initialise requests
        """
        JSONRequests.__init__(self, url, {"cachepath" : None, "pycurl" : usePYCurl, "key" : ckey, "cert" : cert, "capath" : capath})
        self.accept_type = "application/json"
        self["timeout"] = 600

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
            result, status, reason, cached = JSONRequests.makeRequest(
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
        elif status in [502, 503, 504]:
            # There are HTTP errors that CouchDB doesn't raise but can appear
            # in our environment, e.g. behind a proxy. Reraise the HTTPException
            raise
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
    def __init__(self, dbname = 'database', url = 'http://localhost:5984', size = 1000, ckey = None, cert = None):
        """
        A set of queries against a CouchDB database
        """
        check_name(dbname)

        self.name = urllib.quote_plus(dbname)

        CouchDBRequests.__init__(self, url = url, ckey = ckey, cert = cert)
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

        if isinstance(data, type({})):
            data[label] = int(time.time())
        else:
            for doc in data:
                if label not in doc.keys():
                    doc[label] = int(time.time())
        return data

    def queue(self, doc, timestamp = False, viewlist=[], callback = None):
        """
        Queue up a doc for bulk insert. If timestamp = True add a timestamp
        field if one doesn't exist. Use this over commit(timestamp=True) if you
        want to timestamp when a document was added to the queue instead of when
        it was committed
        If a callback is specified then pass it to the commit function if a
        commit is triggered
        """
        if timestamp:
            self.timestamp(doc, timestamp)
        #TODO: Thread this off so that it's non blocking...
        if len(self._queue) >= self._queue_size:
            print 'queue larger than %s records, committing' % self._queue_size
            self.commit(viewlist=viewlist, callback = callback)
        self._queue.append(doc)

    def queueDelete(self, doc, viewlist=[]):
        """
        Queue up a document for deletion
        """
        assert isinstance(doc, type({})), "document not a dictionary"
        # https://issues.apache.org/jira/browse/COUCHDB-1141
        doc = { '_id' : doc['_id'], '_rev' : doc['_rev'], '_deleted' : True }
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
               viewlist=[], callback = None, **data):
        """
        Add doc and/or the contents of self._queue to the database. If
        returndocs is true, return document objects representing what has been
        committed. If timestamp is true timestamp all documents with a unix style
        timestamp - this will be the timestamp of when the commit was called, it
        will not override an existing timestamp field.  If timestamp is a string
        that string will be used as the label for the timestamp.

        The callback function will be called with the documents that trigger a
        conflict when doing the bulk post of the documents in the queue,
        callback functions must accept the database object, the data posted and a row in the
        result from the bulk commit. The callback updates the retval with
        its internal retval

        key, value pairs can be used to pass extra parameters to the bulk doc api
        See http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API

        TODO: restore support for returndocs and viewlist

        Returns a list of good documents
            throws an exception otherwise
        """
        if (doc):
            self.queue(doc, timestamp, viewlist)

        if len(self._queue) == 0:
            return

        if timestamp:
            self.timestamp(self._queue, timestamp)
        # TODO: commit in thread to avoid blocking others
        uri  = '/%s/_bulk_docs/' % self.name

        data['docs'] = list(self._queue)
        retval = self.post(uri , data)
        self._reset_queue()
        for v in viewlist:
            design, view = v.split('/')
            self.loadView(design, view, {'limit': 0})
        if callback:
            for idx, result in enumerate(retval):
                if result.get('error', None) == 'conflict':
                    retval[idx] = callback(self, data, result)

        return retval

    def document(self, id, rev = None):
        """
        Load a document identified by id. You can specify a rev to see an older revision
        of the document. This **should only** be used when resolving conflicts, relying
        on CouchDB revisions for document history is not safe, as any compaction will
        remove the older revisions.
        """
        uri = '/%s/%s' % (self.name, urllib.quote_plus(id))
        if rev:
            uri += '?' + urllib.urlencode({'rev' : rev})
        return Document(id = id, inputDict = self.get(uri))

    def updateDocument(self, doc_id, design, update_func, fields={}, useBody=False):
        """
        Call the update function update_func defined in the design document
        design for the document doc_id with a query string built from fields.

        http://wiki.apache.org/couchdb/Document_Update_Handlers
        """
        # Clean up /'s in the name etc.
        doc_id = urllib.quote_plus(doc_id)
        
        if not useBody:
            updateUri = '/%s/_design/%s/_update/%s/%s?%s' % \
                (self.name, design, update_func, doc_id, urllib.urlencode(fields))
    
            return self.put(uri = updateUri, decode=False)
        else:
            updateUri = '/%s/_design/%s/_update/%s/%s' % \
                (self.name, design, update_func, doc_id)
            return self.put(uri=updateUri, data=fields, decode=False)

    def documentExists(self, id, rev = None):
        """
        Check if a document exists by ID. If specified check that the revision rev exists.
        """
        uri = "/%s/%s" % (self.name, urllib.quote_plus(id))
        if rev:
            uri += '?' + urllib.urlencode({'rev' : rev})
        try:
            self.makeRequest(uri, {}, 'HEAD')
            return True
        except CouchNotFoundError:
            return False

    def delete_doc(self, id, rev = None):
        """
        Immediately delete a document identified by id and rev.
        """
        doc = self.document(id, rev)
        doc.delete()
        return self.commitOne(doc)

    def compact(self, views=[], blocking=False, blocking_poll=5, callback=False):
        """
        Compact the database: http://wiki.apache.org/couchdb/Compaction

        If given, views should be a list of design document name (minus the
        _design/ - e.g. myviews not _design/myviews). For each view in the list
        view compaction will be triggered. Also, if the views list is provided
        _view_cleanup is called to remove old view output.

        If True blocking will cause this call to wait until the compaction is
        completed, polling for status with frequency blocking_poll and calling
        the function specified by callback on each iteration.

        The callback function can be used for logging and could also be used to
        timeout the compaction based on status (e.g. don't time out if compaction
        is less than X% complete. The callback function takes the Database (self)
        as an argument. If the callback function raises an exception the block is
        removed and the compact call returns.
        """
        response = self.post('/%s/_compact' % self.name)
        if len(views) > 0:
            for view in views:
                response[view] = self.post('/%s/_compact/%s' % (self.name, view))
                response['view_cleanup' ] = self.post('/%s/_view_cleanup' % (self.name))

        if blocking:
            while self.info()['compact_running']:
                if callback:
                    try:
                        callback(self)
                    except Exception:
                        return response
                time.sleep(blocking_poll)
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

    def changesWithFilter(self, filter, limit=1000, since=-1):
        """
        Get the changes since sequence number. Store the last sequence value to
        self.last_seq. If the since is negative use self.last_seq.
        """
        if since < 0:
            since = self.last_seq
        data = self.get('/%s/_changes?limit=%s&since=%s&filter=%s' % (self.name, limit, since, filter))
        self.last_seq = data['last_seq']
        return data
    
    def purge(self, data):
        return self.post('/%s/_purge' % self.name, data)
        
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
            # We can't encode the stale option, as it will be converted to '"ok"'
            # which couch barfs on.
            if k == "stale":
                encodedOptions[k] = v
            else:
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

        return retval

    def allDocs(self, options = {}, keys = []):
        """
        Return all the documents in the database
        options is a dict type parameter which can be passed to _all_docs
        id {'startkey': 'a', 'limit':2, 'include_docs': true}
        keys is the list of key (ids) for doc to be returned
        """
        encodedOptions = {}
        for k,v in options.iteritems():
            encodedOptions[k] = self.encode(v)

        if len(keys):
            if (encodedOptions):
                data = urllib.urlencode(encodedOptions)
                return self.post('/%s/_all_docs?%s' % (self.name, data),
                                 {'keys':keys})
            else:
                return self.post('/%s/_all_docs' % self.name,
                                 {'keys':keys})
        else:
            return self.get('/%s/_all_docs' % self.name, encodedOptions)

    def info(self):
        """
        Return information about the databaes (size, number of documents etc).
        """
        return self.get('/%s/' % self.name)

    def addAttachment(self, id, rev, value, name=None, contentType=None, checksum=None, add_checksum=False):
        """
        Add an attachment stored in value to a document identified by id at revision rev.
        If specified the attachement will be uploaded as name, other wise the attachment is
        named "attachment".

        If not set CouchDB will try to determine contentType and default to text/plain.

        If checksum is specified pass this to CouchDB, it will refuse if the MD5 checksum
        doesn't match the one provided. If add_checksum is True calculate the checksum of
        the attachment and pass that into CouchDB for validation. The checksum should be the
        base64 encoded binary md5 (as returned by hashlib.md5().digest())
        """
        if (name == None):
            name = "attachment"
        req_headers = {}

        if add_checksum:
            #calculate base64 encoded MD5
            keyhash = hashlib.md5()
            keyhash.update(str(value))
            req_headers['Content-MD5'] = base64.b64encode(keyhash.digest())
        elif checksum:
            req_headers['Content-MD5'] = checksum
        return self.put('/%s/%s/%s?rev=%s' % (self.name, id, name, rev),
                         value, encode = False,
                         contentType=contentType,
                         incoming_headers = req_headers)

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

class RotatingDatabase(Database):
    """
    A rotating database is actually multiple databases:
      - one active database (self)
      - N inactive databases (waiting to be removed)
      - one archive database
      - one configuration/seed database

    The active database is the one which serves current requests. It is active
    for a certain time window and then archived and marked as inactive.

    Inactive databases no longer recieve queries, although are still available
    on the server. They are queued up for deletion. This allows you to have a
    system where active databases are rotated daily and are kept in the server
    for a week. Inactive databases have a document in them defined as:
        {
          '_id': 'inactive',
          'archived_at': TIMESTAMP,     # added when archived
          'expires_at': TIMESTAMP+delta # added when archived
        }
    which is used to persist state across instatiations of the class.

    The archive database stores the results of views on the active databases
    once they are rotated out of service.

    The configuration/seed database holds the following information:
        * names of known inactive databases
        * name of current active database
        * name of archive database
        * design documents needed to seed new databases

    Once rotated the current active database is made inactive, a new active
    database created, views are copied to the archive database as necessary and
    the inactive databases queued for removal.
    """
    def __init__(self, dbname = 'database', url = 'http://localhost:5984',
                        size = 1000, archivename=None, seedname=None,
                        timing=None, views=[]):
        """
        dbaname:     base name for databases, active databases will have
                     timestamp appended
        url:         url of the CouchDB server
        size:        how big the data queue can get
        archivename: database to archive view results to, default is
                     dbname_archive
        seedname:    database where seed views and configuration/state are held
                     default is $dbname_seedcfg
        timing:      a dict containing two timedeltas 'archive' and 'expire',
                     if not present assume the database will br rotated by
                     external code
        views:       a list of views (design/name) to archive. The assumption
                     is that these views have been loaded into the seed
                     database via couchapp or someother process.
        """
        # Store the base database name
        self.basename = dbname

        # Since we're going to be making databases hold onto a server
        self.server = CouchServer(url)

        # self is the "active" database
        Database.__init__(self, self._get_new_name(), url, size)
        # forcibly make sure I exist
        self.server.connectDatabase(self.name)

        # Set up the databases for the seed
        if not seedname:
            seedname = '%s_seedcfg' % (self.basename)
        self.seed_db =  self.server.connectDatabase(seedname, url, size)

        # TODO: load a rotating DB from the seed db

        # TODO: Maybe call self._rotate() here?

        self.timing = timing

        self.archive_config = {}
        self.archive_db = None
        self.views = []
        if views:
            # If views isn't set in the constructor theres nothing to archive
            if not archivename:
                archivename = '%s_archive' % (self.basename)
            # TODO: check that the views listed exist in the seed
            # TODO: support passing in view options
            self.views = views
            self.archive_db = self.server.connectDatabase(archivename, url, size)
            self.archive_config['views'] = self.views
            self.archive_config['database'] = archivename
            self.archive_config['type'] = 'archive_config'
            self.archive_config['timing'] = str(self.timing)
            # copy views from the seed to the active db
            self._copy_views()
        if self.archive_config:
            # TODO: deal with multiple instances, load from doc?
            self.seed_db.commitOne(self.archive_config)

    def _get_new_name(self):
        return '%s_%s' % (self.basename, int(time.time()))

    def _copy_views(self):
        """
        Copy design documents from self.seed_db to the new active database.
        This means that all views in the design doc are copied, regardless of
        whether they are actually archived.
        """
        for design_to_copy in set(['_design/%s' % design.split('/')[0] for design in self.views]):
            design = self.seed_db.document(design_to_copy)
            del design['_rev']
            self.queue(design)
        self.commit()

    def _rotate(self):
        """
        Rotate the active database:
            1. create the new active database
            2. set self.name to the new database name
            3. write the inactive document to the old active database
            4. write the inactive document to the seed db
        """
        retiring_db = self.server.connectDatabase(self.name)
        # do the switcheroo
        new_active_db = self.server.connectDatabase(self._get_new_name())
        self.name = new_active_db.name
        self._copy_views()
        # "connect" to the old server, write inactive doc
        retiring_db.commitOne({'_id': 'inactive'}, timestamp=True)

        # record new inactive db to config
        # TODO: update function?

        state_doc = {'_id': retiring_db.name, 'rotate_state': 'inactive'}
        if not self.archive_config:
            # Not configured to archive anything, so skip inactive state
            # set the old db as archived instead
            state_doc['rotate_state'] = 'archived'
        self.seed_db.commitOne(state_doc, timestamp=True)


    def _archive(self):
        """
        Archive inactive databases
        """
        if self.archive_config:
            # TODO: This should be a worker thread/pool thingy so it's non-blocking
            for inactive_db in self.inactive_dbs():
                archiving_db = Database(inactive_db, self['host'])
                for view_to_archive in self.views:
                    # TODO: improve handling views and options here
                    design, view = view_to_archive.split('/')
                    for data in archiving_db.loadView(design, view, options={'group':True})['rows']:
                        self.archive_db.queue(data)
                self.archive_db.commit()
                # Now set the inactive view to archived
                db_state = self.seed_db.document(inactive_db)
                db_state['rotate_state'] = 'archived'
                self.seed_db.commit(db_state)

    def _expire(self):
        """
        Delete inactive databases that have expired, and remove state docs.
        """
        now = datetime.now()
        then = now - self.timing['expire']

        options = {'startkey':0, 'endkey':time.mktime(then.timetuple())}
        expired = self._find_dbs_in_state('archived', options)
        for db in expired:
            try:
                self.server.deleteDatabase(db['id'])
            except CouchNotFoundError:
                # if it's gone we don't care
                pass
            db_state = self.seed_db.document(db['id'])
            self.seed_db.queueDelete(db_state)
        self.seed_db.commit()

    def _find_dbs_in_state(self, state, options = {}):
        # TODO: couchapp this, how to make sure that the app is deployed?
        find = {'map':"function(doc) {if(doc.rotate_state == '%s') {emit(doc.timestamp, doc._id);}}" % state}
        uri = '/%s/_temp_view' % self.seed_db.name
        if options:
            uri += '?%s' % urllib.urlencode(options)
        data = self.seed_db.post(uri, find)
        return data['rows']

    def inactive_dbs(self):
        """
        Return a list on inactive databases
        """
        return [doc['value'] for doc in self._find_dbs_in_state('inactive')]

    def archived_dbs(self):
        """
        Return a list of archived databases
        """
        return [doc['value'] for doc in self._find_dbs_in_state('archived')]

    def non_rotating_commit():
        # might need this after all....
        pass

    def makeRequest(self, uri=None, data=None, type='GET', incoming_headers = {},
                     encode=True, decode=True, contentType=None,
                     cache=False, rotate=True):
        """
        Intercept the request, determine if I need to rotate, then carry out the
        request as normal.
        """
        if self.timing and rotate:

            # check to see whether I should rotate the database before processing the request
            db_age = datetime.fromtimestamp(float(self.name.split('_')[-1]))
            db_expires = db_age + self.timing['archive']
            if datetime.now() > db_expires:
                # save the current name for later
                old_db = self.name
                if len(self._queue) > 0:
                    # data I've got queued up should go to the old database
                    # can't call self.commit() due to recursion
                    uri  = '/%s/_bulk_docs/' % self.name
                    data['docs'] = list(self._queue)
                    self.makeRequest(uri, data, 'POST', rotate=False)
                    self._reset_queue()
                self._rotate() # make the new database
                # The uri passed in will be wrong, and the db may no longer exist if it has expired
                # so replacte the old name with the new
                uri.replace(old_db, self.name, 1)
        # write the data to the current database
        Database.makeRequest(self, uri, data, type, incoming_headers, encode, decode, contentType, cache)
        # now do some maintenance on the archived/expired databases
        self._archive()
        self._expire()

class CouchServer(CouchDBRequests):
    """
    An object representing the CouchDB server, use it to list, create, delete
    and connect to databases.

    More info http://wiki.apache.org/couchdb/HTTP_database_API
    """

    def __init__(self, dburl = 'http://localhost:5984', usePYCurl = False, ckey = None, cert = None, capath = None):
        """
        Set up a connection to the CouchDB server
        """
        check_server_url(dburl)
        CouchDBRequests.__init__(self, url = dburl, usePYCurl = usePYCurl, ckey = ckey, cert = cert, capath = capath)
        self.url = dburl
        self.ckey = ckey
        self.cert = cert

    def listDatabases(self):
        "List all the databases the server hosts"
        return self.get('/_all_dbs')

    def createDatabase(self, dbname, size = 1000):
        """
        A database must be named with all lowercase characters (a-z),
        digits (0-9), or any of the _$()+-/ characters and must end with a slash
        in the URL.
        """
        check_name(dbname)

        self.put("/%s" % urllib.quote_plus(dbname))
        # Pass the Database constructor the unquoted name - the constructor will
        # quote it for us.
        return Database(dbname = dbname, url = self.url, size = size, ckey = self.ckey, cert = self.cert)

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
        return Database(dbname = dbname, url = self.url, size = size, ckey = self.ckey, cert = self.cert)

    def replicate(self, source, destination, continuous = False,
                  create_target = False, cancel = False, doc_ids=False,
                  filter = False, query_params = False, useReplicator = False):
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
        if source not in self.listDatabases():
            check_server_url(source)
        if destination not in self.listDatabases():
            if create_target and not destination.startswith("http"):
                check_name(destination)
            else:
                check_server_url(destination)
        if not destination.startswith("http"):
            destination = '%s/%s' % (self.url, destination)
        if not source.startswith("http"):
            source = '%s/%s' % (self.url, source)
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
        if useReplicator:
            self.post('/_replicator', data)
        else:
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


class CouchMonitor(object):
    
    def __init__(self, couchURL):
        if isinstance(couchURL, CouchServer):
            self.couchServer = couchURL
        else:
            self.couchServer = CouchServer(couchURL)
            
        self.replicatorDB = self.couchServer.connectDatabase('_replicator', False)
        # this is set {source: {taget: update_sequence}}
        self.previousUpdateSequence  = {}
    
    def deleteReplicatorDocs(self, source, target, repDocs = None):
        if repDocs == None:
            repDocs = self.replicatorDB.allDocs(options={'include_docs': True})['rows']
        
        filteredDocs = self._filterReplicationDocs(repDocs, source, target)
        if len(filteredDocs) == 0:
            return 
        for doc in filteredDocs:
            self.replicatorDB.queueDelete(doc)
        return self.replicatorDB.commit()
    
    def _filterReplicationDocs(self, repDocs, source, target):
        filteredDocs = []
        for j in repDocs:
            if not j['id'].startswith('_'):
                if j['doc']['source'] == source and j['doc']['target'] == target:
                    doc = {}
                    doc["_id"]  = j['id']
                    doc["_rev"] = j['value']['rev']
                    if doc.has_key("_replication_state"):
                        doc["_replication_state"] = j["doc"]["_replication_state"]
                    else:
                        logging.error("""replication failed from %s to %s 
                                         couch server manually need to be restarted""" % (source, target))
                    filteredDocs.append(doc)
        return filteredDocs
    
    def getPreviousUpdateSequence(self, source, target):
        targetDict = self.previousUpdateSequence.setdefault(source, {})
        return targetDict.setdefault(target, 0)
    
    def setPreviousUpdateSequence(self, source, target, updateSeq):
        self.previousUpdateSequence[source][target] = updateSeq
        
    def recoverReplicationErrors(self, source, target, filter = False, 
                                 query_params = False,
                                 checkUpdateSeq = True,
                                 continuous = True):
        previousUpdateNum = self.getPreviousUpdateSequence(source, target)
        
        couchInfo = self.checkCouchServerStatus(source, target, previousUpdateNum, 
                                                checkUpdateSeq)

        if (couchInfo['status'] == 'error'):
            logging.info("Deleting the replicator documents from %s..." % source)
            self.deleteReplicatorDocs(source, target)
            logging.info("Setting the replication from %s ..." % source)
            self.couchServer.replicate(source, target, filter = filter, 
                                           query_params = query_params,
                                           continuous = continuous,
                                           useReplicator = True)
            
            couchInfo = self.checkCouchServerStatus(source, target, previousUpdateNum,
                                                    checkUpdateSeq)
        #if (couchInfo['status'] != 'down'):
        #    restart couch server
        return couchInfo

    
    def checkCouchServerStatus(self, source, target, previousUpdateNum, checkUpdateSeq):
        try:
            if checkUpdateSeq:
                dbInfo =  self.couchServer.get("/%s" % source)
            else:
                dbInfo = None
            activeTasks = self.couchServer.get("/_active_tasks")
            replicationFlag = False
            passwdStrippedURL = target.split("@")[-1]
            
            for activeStatus in activeTasks:
                if activeStatus["type"].lower() == "replication":
                    if passwdStrippedURL in activeStatus.get("task", "").split("->")[-1]:
                        replicationFlag = self.checkReplicationStatus(activeStatus, 
                                                    dbInfo, source, target, 
                                                    previousUpdateNum, checkUpdateSeq)
                        break
                    elif passwdStrippedURL in activeStatus.get("target", ""):
                        replicationFlag = self.checkReplicationStatusCouch15(activeStatus, 
                                                    dbInfo, source, target, 
                                                    previousUpdateNum, checkUpdateSeq)
                        break
            
            if replicationFlag:
                return {'status': 'ok'}
            else:
                return {'status':'error', 'error_message': "replication stopped"}
        except Exception, ex:
            import traceback
            msg = traceback.format_exc() 
            logging.error(msg)
            return {'status':'down', 'error_message': str(ex)}
    
    def checkReplicationStatus(self, activeStatus, dbInfo, source, target, 
                               previousUpdateNum, checkUpdateSeq):
        """
        adhog way to check the replication
        """
        
        logging.info("checking the replication status")
        if "Starting" in activeStatus["status"]:
            logging.info("Starting status: ok")
            return True
        elif "update" in activeStatus["status"]:
            updateNum = int(activeStatus["status"].split('#')[-1])
            self.setPreviousUpdateSequence(source, target, updateNum)
            
            if not checkUpdateSeq:
                logging.warning("""Update sequence is not checked\n" /
                                   replication from %s to %s""" % (
                                replaceToSantizeURL(source), replaceToSantizeURL(target)))
                return True
            elif updateNum == dbInfo["update_seq"] or updateNum > previousUpdateNum:
                logging.info("update upto date: ok")
                return True
            else:
                return False
        return False
    
    def checkReplicationStatusCouch15(self, activeStatus, dbInfo, source, target, 
                                      previousUpdateNum, checkUpdateSeq):
        """
        adhog way to check the replication
        """
        
        logging.info("checking the replication status")
        #if activeStatus["started_on"]:
        #   logging.info("Starting status: ok")
        #    return True
        if activeStatus["updated_on"]:
            updateNum = int(activeStatus["source_seq"])
            self.setPreviousUpdateSequence(source, target, updateNum)
            if not checkUpdateSeq:
                logging.warning("Need to check replication from %s to %s" % (
                                replaceToSantizeURL(source), replaceToSantizeURL(target)))
                return True
            elif updateNum == dbInfo["update_seq"] or updateNum > previousUpdateNum:
                logging.info("update upto date: ok")
                return True
            else:
                return False
        return False
