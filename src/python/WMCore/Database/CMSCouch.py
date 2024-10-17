#!/usr/bin/env python
"""
_CMSCouch_

A simple API to CouchDB that sends HTTP requests to the REST interface.

http://wiki.apache.org/couchdb/API_Cheatsheet

NOT A THREAD SAFE CLASS.
"""
from __future__ import print_function, division
from builtins import str as newstr, bytes as newbytes, object
from Utils.Utilities import decodeBytesToUnicode, encodeUnicodeToBytes, decodeBytesToUnicodeConditional
from Utils.PythonVersion import PY3

from future import standard_library
standard_library.install_aliases()
from future.utils import viewitems
import urllib.request, urllib.parse, urllib.error

import base64
import hashlib
import json
import logging
import re
import time
import sys
from datetime import datetime
from http.client import HTTPException

from Utils.IteratorTools import grouper, nestedDictUpdate
from WMCore.Lexicon import sanitizeURL
from WMCore.Services.Requests import JSONRequests


def check_name(dbname):
    match = re.match("^[a-z0-9_$()+-/]+$", urllib.parse.unquote_plus(dbname))
    if not match:
        msg = '%s is not a valid database name'
        raise ValueError(msg % urllib.parse.unquote_plus(dbname))


def check_server_url(srvurl):
    good_name = srvurl.startswith('http://') or srvurl.startswith('https://')
    if not good_name:
        raise ValueError('You must include http(s):// in your servers address')


PY3_STR_DECODER = lambda x: decodeBytesToUnicodeConditional(x, condition=PY3)


class Document(dict):
    """
    Document class is the instantiation of one document in the CouchDB
    """

    def __init__(self, id=None, inputDict=None):
        """
        Initialise our Document object - a dictionary which has an id field
        inputDict - input dictionary to initialise this instance
        """
        inputDict = inputDict or {}
        dict.__init__(self)
        self.update(inputDict)
        if id:
            self.setdefault("_id", id)

    def delete(self):
        """
        Mark the document as deleted
        """
        # https://issues.apache.org/jira/browse/COUCHDB-1141
        deletedDict = {'_id': self['_id'], '_rev': self['_rev'], '_deleted': True}
        self.update(deletedDict)
        for key in list(self.keys()):
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

    def __init__(self, url='http://localhost:5984', usePYCurl=True, ckey=None, cert=None, capath=None):
        """
        Initialise requests
        """
        JSONRequests.__init__(self, url,
                              {"cachepath": None, "pycurl": usePYCurl, "key": ckey, "cert": cert, "capath": capath})
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

    def makeRequest(self, uri=None, data=None, type='GET', incoming_headers=None,
                    encode=True, decode=True, contentType=None, cache=False):
        """
        Make the request, handle any failed status, return just the data (for
        compatibility). By default do not cache the response.

        TODO: set caching in the calling methods.
        """
        incoming_headers = incoming_headers or {}
        incoming_headers.update(self.additionalHeaders)
        try:
            if not cache:
                incoming_headers.update({'Cache-Control': 'no-cache'})
            result, status, reason, cached = JSONRequests.makeRequest(
                    self, uri, data, type, incoming_headers,
                    encode, decode, contentType)
        except HTTPException as e:
            self.checkForCouchError(getattr(e, "status", None),
                                    getattr(e, "reason", None),
                                    data,
                                    getattr(e, "result", None))

        return result

    def checkForCouchError(self, status, reason, data=None, result=None):
        """
        _checkForCouchError_

        Check the HTTP status and raise an appropriate exception.
        """
        if status == 400:
            raise CouchBadRequestError(reason, data, result, status)
        elif status == 401:
            raise CouchUnauthorisedError(reason, data, result, status)
        elif status == 403:
            raise CouchForbidden(reason, data, result, status)
        elif status == 404:
            raise CouchNotFoundError(reason, data, result, status)
        elif status == 405:
            raise CouchNotAllowedError(reason, data, result, status)
        elif status == 406:
            raise CouchNotAcceptableError(reason, data, result, status)
        elif status == 409:
            raise CouchConflictError(reason, data, result, status)
        elif status == 410:
            raise CouchFeatureGone(reason, data, result, status)
        elif status == 412:
            raise CouchPreconditionFailedError(reason, data, result, status)
        elif status == 413:
            raise CouchRequestTooLargeError(reason, data, result, status)
        elif status == 416:
            raise CouchRequestedRangeNotSatisfiableError(reason, data, result, status)
        elif status == 417:
            raise CouchExpectationFailedError(reason, data, result, status)
        elif status == 500:
            raise CouchInternalServerError(reason, data, result, status)
        elif status in [502, 503, 504]:
            # There are HTTP errors that CouchDB doesn't raise but can appear
            # in our environment, e.g. behind a proxy. Reraise the HTTPException
            raise CouchError(reason, data, result, status)
        else:
            # We have a new error status, log it
            raise CouchError(reason, data, result, status)



class Database(CouchDBRequests):
    """
    Object representing a connection to a CouchDB Database instance.
    TODO: implement COPY and MOVE calls.
    TODO: remove leading whitespace when committing a view
    """

    def __init__(self, dbname='database', url='http://localhost:5984', size=1000, ckey=None, cert=None):
        """
        A set of queries against a CouchDB database
        """
        check_name(dbname)

        self.name = urllib.parse.quote_plus(dbname)

        CouchDBRequests.__init__(self, url=url, ckey=ckey, cert=cert)
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
        if label is True:
            label = 'timestamp'

        if isinstance(data, type({})):
            data[label] = int(time.time())
        else:
            for doc in data:
                if label not in doc:
                    doc[label] = int(time.time())
        return data

    def getQueueSize(self):
        """
        Return the current size of the queue, i.e., how
        many documents are already queued up
        """
        return len(self._queue)

    def queue(self, doc, timestamp=False, viewlist=None, callback=None):
        """
        Queue up a doc for bulk insert. If timestamp = True add a timestamp
        field if one doesn't exist. Use this over commit(timestamp=True) if you
        want to timestamp when a document was added to the queue instead of when
        it was committed
        If a callback is specified then pass it to the commit function if a
        commit is triggered
        """
        viewlist = viewlist or []
        if timestamp:
            self.timestamp(doc, timestamp)
        # TODO: Thread this off so that it's non blocking...
        if self.getQueueSize() >= self._queue_size:
            logging.warning('queue larger than %s records, committing', self._queue_size)
            self.commit(viewlist=viewlist, callback=callback)
        self._queue.append(doc)

    def queueDelete(self, doc):
        """
        Queue up a document for deletion
        """
        assert isinstance(doc, type({})), "document not a dictionary"
        # https://issues.apache.org/jira/browse/COUCHDB-1141
        doc = {'_id': doc['_id'], '_rev': doc['_rev'], '_deleted': True}
        self.queue(doc)

    def commitOne(self, doc, timestamp=False, viewlist=None):
        """
        Helper function for when you know you only want to insert one doc
        additionally keeps from having to rewrite ConfigCache to handle the
        new commit function's semantics
        """
        viewlist = viewlist or []
        uri = '/%s/_bulk_docs/' % self.name
        if timestamp:
            self.timestamp(doc, timestamp)

        data = {'docs': [doc]}
        retval = self.post(uri, data)
        for v in viewlist:
            design, view = v.split('/')
            self.loadView(design, view, {'limit': 0})
        return retval

    def commit(self, doc=None, returndocs=False, timestamp=False,
               viewlist=None, callback=None, **data):
        """
        Add doc and/or the contents of self._queue to the database.
        If timestamp is true timestamp all documents with a unix style
        timestamp - this will be the timestamp of when the commit was called, it
        will not override an existing timestamp field.  If timestamp is a string
        that string will be used as the label for the timestamp.

        The callback function will be called with the documents that trigger a
        conflict when doing the bulk post of the documents in the queue,
        callback functions must accept the database object, the data posted and a row in the
        result from the bulk commit. The callback updates the retval with
        its internal retval

        key, value pairs can be used to pass extra parameters to the bulk doc api
        See https://docs.couchdb.org/en/latest/api/database/bulk-api.html#db-bulk-docs

        TODO: restore support for returndocs and viewlist

        Returns a list of good documents
            throws an exception otherwise
        """
        viewlist = viewlist or []
        if doc:
            self.queue(doc, timestamp, viewlist)

        if not self._queue:
            return

        if timestamp:
            self.timestamp(self._queue, timestamp)
        # TODO: commit in thread to avoid blocking others
        uri = '/%s/_bulk_docs/' % self.name

        data['docs'] = list(self._queue)
        retval = self.post(uri, data)
        self._reset_queue()
        for v in viewlist:
            design, view = v.split('/')
            self.loadView(design, view, {'limit': 0})
        if callback:
            for idx, result in enumerate(retval):
                if result.get('error', None) == 'conflict':
                    retval[idx] = callback(self, data, result)

        return retval

    def document(self, id, rev=None):
        """
        Load a document identified by id. You can specify a rev to see an older revision
        of the document. This **should only** be used when resolving conflicts, relying
        on CouchDB revisions for document history is not safe, as any compaction will
        remove the older revisions.
        """
        uri = '/%s/%s' % (self.name, urllib.parse.quote_plus(id))
        if rev:
            uri += '?' + urllib.parse.urlencode({'rev': rev})
        return Document(id=id, inputDict=self.get(uri))

    def updateDocument(self, doc_id, design, update_func, fields=None, useBody=False):
        """
        Call the update function update_func defined in the design document
        design for the document doc_id with a query string built from fields.

        http://wiki.apache.org/couchdb/Document_Update_Handlers
        """
        fields = fields or {}
        # Clean up /'s in the name etc.
        doc_id = urllib.parse.quote_plus(doc_id)

        if not useBody:
            updateUri = '/%s/_design/%s/_update/%s/%s?%s' % \
                        (self.name, design, update_func, doc_id, urllib.parse.urlencode(fields))

            return self.put(uri=updateUri, decode=PY3_STR_DECODER)
        else:
            updateUri = '/%s/_design/%s/_update/%s/%s' % \
                        (self.name, design, update_func, doc_id)
            return self.put(uri=updateUri, data=fields, decode=PY3_STR_DECODER)

    def updateBulkDocuments(self, doc_ids, paramsToUpdate, updateLimits=1000):

        uri = '/%s/_bulk_docs/' % self.name
        conflictDocIDs = []
        for ids in grouper(doc_ids, updateLimits):
            # get original documens
            docs = self.allDocs(options={"include_docs": True}, keys=ids)['rows']
            data = {}
            data['docs'] = []
            for j in docs:
                doc = {}
                doc.update(j['doc'])
                nestedDictUpdate(doc, paramsToUpdate)
                data['docs'].append(doc)

            if data['docs']:
                retval = self.post(uri, data)
                for result in retval:
                    if result.get('error', None) == 'conflict':
                        conflictDocIDs.append(result['id'])

        return conflictDocIDs

    def updateBulkDocumentsWithConflictHandle(self, doc_ids, updateParams, updateLimits=1000, maxConflictLimit=10):
        """
        param: doc_ids: list couch doc ids for updates, shouldn't contain any duplicate or empty string
        param: updateParams: dictionary of parameters to be updated.
        param: updateLimits: number of documents in one commit
        pram maxConflictLimit: number of conflicts fix tries before we give up to fix it to prevent infinite calls
        """
        conflictDocIDs = self.updateBulkDocuments(doc_ids, updateParams, updateLimits)
        if conflictDocIDs:
            # wait a second before trying again for the confict documents
            if maxConflictLimit == 0:
                return conflictDocIDs
            time.sleep(1)
            self.updateBulkDocumentsWithConflictHandle(conflictDocIDs, updateParams,
                                                       maxConflictLimit=maxConflictLimit - 1)
        return []

    def putDocument(self, doc_id, fields):
        """
        Call the update function update_func defined in the design document
        design for the document doc_id with a query string built from fields.

        http://wiki.apache.org/couchdb/Document_Update_Handlers
        """
        # Clean up /'s in the name etc.
        doc_id = urllib.parse.quote_plus(doc_id)

        updateUri = '/%s/%s' % (self.name, doc_id)
        return self.put(uri=updateUri, data=fields, decode=PY3_STR_DECODER)

    def documentExists(self, id, rev=None):
        """
        Check if a document exists by ID. If specified check that the revision rev exists.
        """
        uri = "/%s/%s" % (self.name, urllib.parse.quote_plus(id))
        if rev:
            uri += '?' + urllib.parse.urlencode({'rev': rev})
        try:
            self.makeRequest(uri, {}, 'HEAD')
            return True
        except CouchNotFoundError:
            return False

    def delete_doc(self, id, rev=None):
        """
        Immediately delete a document identified by id and rev.
        If revision is not provided, we need to first fetch this
        document to read the current revision number.

        :param id: string with the document name
        :param rev: string with the revision number
        :return: an empty dictionary if it fails to fetch the document,
            or a dictionary with the deletion outcome, e.g.:
            {'ok': True, 'id': 'doc_name', 'rev': '3-f68156d'}
        """
        uri = '/%s/%s' % (self.name, urllib.parse.quote_plus(id))
        if not rev:
            # then we need to fetch the latest revision number
            doc = self.getDoc(id)
            if "_rev" not in doc:
                logging.warning("Failed to retrieve doc id: %s for deletion.", id)
                return doc
            rev = doc["_rev"]
        uri += '?' + urllib.parse.urlencode({'rev': rev})
        return self.delete(uri)

    def compact(self, views=None, blocking=False, blocking_poll=5, callback=False):
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
        views = views or []
        response = self.post('/%s/_compact' % self.name)
        if views:
            for view in views:
                response[view] = self.post('/%s/_compact/%s' % (self.name, view))
                response['view_cleanup'] = self.post('/%s/_view_cleanup' % (self.name))

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

    def loadView(self, design, view, options=None, keys=None):
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
        options = options or {}
        keys = keys or []
        encodedOptions = {}
        for k, v in viewitems(options):
            # We can't encode the stale option, as it will be converted to '"ok"'
            # which couch barfs on.
            if k == "stale":
                encodedOptions[k] = v
            else:
                encodedOptions[k] = self.encode(v)

        if keys:
            if encodedOptions:
                data = urllib.parse.urlencode(encodedOptions)
                retval = self.post('/%s/_design/%s/_view/%s?%s' % \
                                   (self.name, design, view, data), {'keys': keys})
            else:
                retval = self.post('/%s/_design/%s/_view/%s' % \
                                   (self.name, design, view), {'keys': keys})
        else:
            retval = self.get('/%s/_design/%s/_view/%s' % \
                              (self.name, design, view), encodedOptions)
        if 'error' in retval:
            raise RuntimeError("Error in CouchDB: viewError '%s' reason '%s'" % \
                               (retval['error'], retval['reason']))
        else:
            return retval

    def loadList(self, design, list, view, options=None, keys=None):
        """
        Load data from a list function. This returns data that hasn't been
        decoded, since a list can return data in any format. It is expected that
        the caller of this function knows what data is being returned and how to
        deal with it appropriately.
        """
        options = options or {}
        keys = keys or []
        encodedOptions = {}
        for k, v in viewitems(options):
            encodedOptions[k] = self.encode(v)

        if keys:
            if encodedOptions:
                data = urllib.parse.urlencode(encodedOptions)
                retval = self.post('/%s/_design/%s/_list/%s/%s?%s' % \
                                   (self.name, design, list, view, data), {'keys': keys},
                                   decode=PY3_STR_DECODER)
            else:
                retval = self.post('/%s/_design/%s/_list/%s/%s' % \
                                   (self.name, design, list, view), {'keys': keys},
                                   decode=PY3_STR_DECODER)
        else:
            retval = self.get('/%s/_design/%s/_list/%s/%s' % \
                              (self.name, design, list, view), encodedOptions,
                              decode=PY3_STR_DECODER)

        return retval

    def getDoc(self, docName):
        """
        Return a single document from the database.
        """
        try:
            return self.get('/%s/%s' % (self.name, docName))
        except CouchError as e:
            # if empty dict, then doc does not exist in the db
            if getattr(e, "data", None) == {}:
                return {}
            self.checkForCouchError(getattr(e, "status", None), getattr(e, "reason", None))

    def allDocs(self, options=None, keys=None):
        """
        Return all the documents in the database
        options is a dict type parameter which can be passed to _all_docs
        id {'startkey': 'a', 'limit':2, 'include_docs': true}
        keys is the list of key (ids) for doc to be returned
        """
        options = options or {}
        keys = keys or []
        encodedOptions = {}
        for k, v in viewitems(options):
            encodedOptions[k] = self.encode(v)

        if keys:
            if encodedOptions:
                data = urllib.parse.urlencode(encodedOptions)
                return self.post('/%s/_all_docs?%s' % (self.name, data),
                                 {'keys': keys})
            else:
                return self.post('/%s/_all_docs' % self.name,
                                 {'keys': keys})
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
        if name is None:
            name = "attachment"
        req_headers = {}

        if add_checksum:
            # calculate base64 encoded MD5
            keyhash = hashlib.md5()
            value_str = str(value) if not isinstance(value, (newstr, newbytes)) else value
            keyhash.update(encodeUnicodeToBytes(value_str))
            content_md5 = base64.b64encode(keyhash.digest())
            req_headers['Content-MD5'] = decodeBytesToUnicode(content_md5) if PY3 else content_md5
        elif checksum:
            req_headers['Content-MD5'] = decodeBytesToUnicode(checksum) if PY3 else checksum
        return self.put('/%s/%s/%s?rev=%s' % (self.name, id, name, rev),
                        value, encode=False,
                        contentType=contentType,
                        incoming_headers=req_headers)

    def getAttachment(self, id, name="attachment"):
        """
        _getAttachment_

        Retrieve an attachment for a couch document.
        """
        url = "/%s/%s/%s" % (self.name, id, name)
        attachment = self.get(url, None, encode=False, decode=PY3_STR_DECODER)

        # there has to be a better way to do this but if we're not de-jsoning
        # the return values, then this is all I can do for error checking,
        # right?
        # TODO: MAKE BETTER ERROR HANDLING
        if (attachment.find('{"error":"not_found","reason":"deleted"}') != -1):
            raise RuntimeError("File not found, deleted")
        if id == "nonexistantid":
            print(attachment)
        return attachment

    def bulkDeleteByIDs(self, ids):
        """
        delete bulk documents
        """
        # do the safety check other wise it will delete whole db.
        if not isinstance(ids, list):
            raise RuntimeError("Bulk delete requires a list of ids, wrong data type")
        if not ids:
            return None

        docs = self.allDocs(keys=ids)['rows']
        for j in docs:
            doc = {}
            if "id" not in j:
                print("Document not found: %s" % j)
                continue
            doc["_id"] = j['id']
            doc["_rev"] = j['value']['rev']
            self.queueDelete(doc)
        return self.commit()


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

    def __init__(self, dbname='database', url='http://localhost:5984',
                 size=1000, archivename=None, seedname=None,
                 timing=None, views=None):
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
        views = views or []
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
        self.seed_db = self.server.connectDatabase(seedname, url, size)

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
                    for data in archiving_db.loadView(design, view, options={'group': True})['rows']:
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

        options = {'startkey': 0, 'endkey': int(time.mktime(then.timetuple()))}
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

    def _create_design_doc(self):
        """Create a design doc with a view for the rotate state"""
        tempDesignDoc = {'views': {
                             'rotateState': {
                                 'map': "function(doc) {emit(doc.timestamp, doc.rotate_state, doc._id);}"
                                            },
                                   }
                         }
        self.seed_db.put('/%s/_design/TempDesignDoc' % self.seed_db.name, tempDesignDoc)

    def _find_dbs_in_state(self, state, options=None):
        """Creates a design document with a single (temporary) view in it"""
        options = options or {}
        if self.seed_db.documentExists("_design/TempDesignDoc"):
            logging.info("Skipping designDoc creation because it already exists!")
        else:
            self._create_design_doc()

        data = self.seed_db.loadView("TempDesignDoc", "rotateState", options=options)
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

    def makeRequest(self, uri=None, data=None, type='GET', incoming_headers=None,
                    encode=True, decode=True, contentType=None,
                    cache=False, rotate=True):
        """
        Intercept the request, determine if I need to rotate, then carry out the
        request as normal.
        """
        incoming_headers = incoming_headers or {}
        if self.timing and rotate:

            # check to see whether I should rotate the database before processing the request
            db_age = datetime.fromtimestamp(float(self.name.split('_')[-1]))
            db_expires = db_age + self.timing['archive']
            if datetime.now() > db_expires:
                # save the current name for later
                old_db = self.name
                if self._queue:
                    # data I've got queued up should go to the old database
                    # can't call self.commit() due to recursion
                    uri = '/%s/_bulk_docs/' % self.name
                    data['docs'] = list(self._queue)
                    self.makeRequest(uri, data, 'POST', rotate=False)
                    self._reset_queue()
                self._rotate()  # make the new database
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

    def __init__(self, dburl='http://localhost:5984', usePYCurl=True, ckey=None, cert=None, capath=None):
        """
        Set up a connection to the CouchDB server
        """
        check_server_url(dburl)
        CouchDBRequests.__init__(self, url=dburl, usePYCurl=usePYCurl, ckey=ckey, cert=cert, capath=capath)
        self.url = dburl
        self.ckey = ckey
        self.cert = cert

    def getCouchWelcome(self):
        """
        Retrieve CouchDB welcome information (which includes the version number)
        :return: a dictionary
        """
        return self.get('')

    def listDatabases(self):
        "List all the databases the server hosts"
        return self.get('/_all_dbs')

    def createDatabase(self, dbname, size=1000):
        """
        A database must be named with all lowercase characters (a-z),
        digits (0-9), or any of the _$()+-/ characters and must end with a slash
        in the URL.
        """
        check_name(dbname)

        self.put("/%s" % urllib.parse.quote_plus(dbname))
        # Pass the Database constructor the unquoted name - the constructor will
        # quote it for us.
        return Database(dbname=dbname, url=self.url, size=size, ckey=self.ckey, cert=self.cert)

    def deleteDatabase(self, dbname):
        """Delete a database from the server"""
        check_name(dbname)
        dbname = urllib.parse.quote_plus(dbname)
        if "cmsweb" in self.url:
            msg = f"You can't be serious that you want to delete a PRODUCTION database!!! "
            msg += f"At url: {self.url}, for database name: {dbname}. Bailing out!"
            raise RuntimeError(msg)
        return self.delete("/%s" % dbname)

    def connectDatabase(self, dbname='database', create=True, size=1000):
        """
        Return a Database instance, pointing to a database in the server. If the
        database doesn't exist create it if create is True.
        """
        check_name(dbname)
        if create and dbname not in self.listDatabases():
            return self.createDatabase(dbname)
        return Database(dbname=dbname, url=self.url, size=size, ckey=self.ckey, cert=self.cert)

    def replicate(self, source, destination, continuous=False,
                  create_target=False, cancel=False, doc_ids=False,
                  filter=False, query_params=False, sleepSecs=0, selector=False):
        """
        Trigger replication between source and destination. CouchDB options are
        defined in: https://docs.couchdb.org/en/3.1.2/api/server/common.html#replicate
        with further details in: https://docs.couchdb.org/en/stable/replication/replicator.html

        Source and destination need to be appropriately urlquoted after the port
        number. E.g. if you have a database with /'s in the name you need to
        convert them into %2F's.

        TODO: Improve source/destination handling - can't simply URL quote,
        though, would need to decompose the URL and rebuild it.

        :param source: string with the source url to replicate data from
        :param destination: string with the destination url to replicate data to
        :param continuous: boolean to perform a continuous replication or not
        :param create_target: boolean to create the target database, if non-existent
        :param cancel: boolean to stop a replication (but we better just delete the doc!)
        :param doc_ids: a list of specific doc ids that we would like to replicate
        :param filter: string with the name of the filter function to be used. Note that
                       this filter is expected to have been defined in the design doc.
        :param query_params: dictionary of parameters to pass over to the filter function
        :param sleepSecs: amount of seconds to sleep after the replication job is created
        :param selector: a new'ish feature for filter functions in Erlang
        :return: status of the replication creation
        """
        listDbs = self.listDatabases()
        if source not in listDbs:
            check_server_url(source)
        if destination not in listDbs:
            if create_target and not destination.startswith("http"):
                check_name(destination)
            else:
                check_server_url(destination)

        if not destination.startswith("http"):
            destination = '%s/%s' % (self.url, destination)
        if not source.startswith("http"):
            source = '%s/%s' % (self.url, source)
        data = {"source": source, "target": destination}
        # There must be a nicer way to do this, but I've not had coffee yet...
        if continuous: data["continuous"] = continuous
        if create_target: data["create_target"] = create_target
        if cancel: data["cancel"] = cancel
        if doc_ids: data["doc_ids"] = doc_ids
        if filter:
            data["filter"] = filter
            if query_params:
                data["query_params"] = query_params
        if selector: data["selector"] = selector

        resp = self.post('/_replicator', data)
        # Sleep required for CouchDB 3.x unit tests
        time.sleep(sleepSecs)
        return resp

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

    def __init__(self, reason, data, result, status=None):
        Exception.__init__(self)
        self.reason = reason
        self.data = data
        self.result = result
        self.type = "CouchError"
        self.status = status

    def __str__(self):
        """Stringify the error"""
        errorMsg = ""
        if self.type == "CouchError":
            errorMsg += "A NEW COUCHDB ERROR TYPE/STATUS HAS BEEN FOUND! "
            errorMsg += "UPDATE CMSCOUCH.PY IMPLEMENTATION WITH A NEW COUCH ERROR/STATUS! "
            errorMsg += f"Status: {self.status}\n"
        errorMsg += f"Error type: {self.type}, Status code: {self.status}, "
        errorMsg += f"Reason: {self.reason}, Data: {repr(self.data)}"
        return errorMsg


class CouchBadRequestError(CouchError):
    def __init__(self, reason, data, result, status):
        CouchError.__init__(self, reason, data, result, status)
        self.type = "CouchBadRequestError"


class CouchUnauthorisedError(CouchError):
    def __init__(self, reason, data, result, status):
        CouchError.__init__(self, reason, data, result, status)
        self.type = "CouchUnauthorisedError"


class CouchNotFoundError(CouchError):
    def __init__(self, reason, data, result, status):
        CouchError.__init__(self, reason, data, result, status)
        self.type = "CouchNotFoundError"


class CouchNotAllowedError(CouchError):
    def __init__(self, reason, data, result, status):
        CouchError.__init__(self, reason, data, result, status)
        self.type = "CouchNotAllowedError"

class CouchNotAcceptableError(CouchError):
    def __init__(self, reason, data, result, status):
        CouchError.__init__(self, reason, data, result, status)
        self.type = "CouchNotAcceptableError"

class CouchConflictError(CouchError):
    def __init__(self, reason, data, result, status):
        CouchError.__init__(self, reason, data, result, status)
        self.type = "CouchConflictError"


class CouchFeatureGone(CouchError):
    def __init__(self, reason, data, result, status):
        CouchError.__init__(self, reason, data, result, status)
        self.type = "CouchFeatureGone"


class CouchPreconditionFailedError(CouchError):
    def __init__(self, reason, data, result, status):
        CouchError.__init__(self, reason, data, result, status)
        self.type = "CouchPreconditionFailedError"


class CouchRequestTooLargeError(CouchError):
    def __init__(self, reason, data, result, status):
        # calculate the size of this JSON serialized object
        docSize = sys.getsizeof(json.dumps(data))
        errorMsg = f"Document has {docSize} bytes and it's too large to be accepted by CouchDB. "
        errorMsg += f"Check the CouchDB configuration to see the current value "
        errorMsg += f"under 'couchdb.max_document_size' (default is 8M bytes)."
        CouchError.__init__(self, reason, errorMsg, result, status)
        self.type = "CouchRequestTooLargeError"


class CouchExpectationFailedError(CouchError):
    def __init__(self, reason, data, result, status):
        CouchError.__init__(self, reason, data, result, status)
        self.type = "CouchExpectationFailedError"

class CouchRequestedRangeNotSatisfiableError(CouchError):
    def __init__(self, reason, data, result, status):
        CouchError.__init__(self, reason, data, result, status)
        self.type = "CouchRequestedRangeNotSatisfiableError"


class CouchInternalServerError(CouchError):
    def __init__(self, reason, data, result, status):
        CouchError.__init__(self, reason, data, result, status)
        self.type = "CouchInternalServerError"


class CouchForbidden(CouchError):
    def __init__(self, reason, data, result, status):
        CouchError.__init__(self, reason, data, result, status)
        self.type = "CouchForbidden"


class CouchMonitor(object):
    def __init__(self, couchURL):
        if isinstance(couchURL, CouchServer):
            self.couchServer = couchURL
        else:
            self.couchServer = CouchServer(couchURL)

        self.replicatorDB = self.couchServer.connectDatabase('_replicator', False)

        # use the CouchDB version to decide which APIs and schema is available
        couchInfo = self.couchServer.getCouchWelcome()
        self.couchVersion = couchInfo.get("version")

    def deleteReplicatorDocs(self, source=None, target=None, repDocs=None):
        if repDocs is None:
            repDocs = self.replicatorDB.allDocs(options={'include_docs': True})['rows']

        filteredDocs = self._filterReplicationDocs(repDocs, source, target)
        if not filteredDocs:
            return
        for doc in filteredDocs:
            self.replicatorDB.queueDelete(doc)
        return self.replicatorDB.commit()

    def _filterReplicationDocs(self, repDocs, source, target):
        filteredDocs = []
        for j in repDocs:
            if '_design' not in j['id']:
                if (source is None and target is None) or \
                        (j['doc']['source'] == source and j['doc']['target'] == target):
                    doc = {}
                    doc["_id"] = j['id']
                    doc["_rev"] = j['value']['rev']
                    filteredDocs.append(doc)
        return filteredDocs

    def getActiveTasks(self):
        """
        Return all the active tasks in Couch (compaction, replication, indexing, etc)
        :return: a list with the current active tasks

        For further information:
            https://docs.couchdb.org/en/3.1.2/api/server/common.html#active-tasks
        """
        return self.couchServer.get("/_active_tasks")

    def getSchedulerJobs(self):
        """
        Return all replication jobs created either via _replicate or _replicator dbs.
        It does not include replications that have either completed or failed.
        :return: a list with the current replication jobs

        For further information:
            https://docs.couchdb.org/en/3.1.2/api/server/common.html#api-server-scheduler-jobs
        """
        resp = []
        data = self.couchServer.get("/_scheduler/jobs")
        return data.get("jobs", resp)

    def getSchedulerDocs(self):
        """
        Return all replication documents and their states, even if they have completed or
        failed.
        :return: a list with the current replication docs

        Replication states can be found at:
            https://docs.couchdb.org/en/3.1.2/replication/replicator.html#replicator-states
        For further information:
            https://docs.couchdb.org/en/3.1.2/api/server/common.html#api-server-scheduler-docs
        """
        # NOTE: if there are no docs, this call can give a response like:
        # {"error":"not_found","reason":"Database does not exist."}
        resp = []
        try:
            data = self.couchServer.get("/_scheduler/docs")
        except CouchNotFoundError as exc:
            logging.warning("/_scheduler/docs API returned: %s", getattr(exc, "result", ""))
            return resp
        return data.get("docs", resp)

    def checkCouchReplications(self, replicationsList):
        """
        Check whether the list of expected replications exist in CouchDB
        and also check their status.

        :param replicationsList: a list of dictionary with the replication
            document setup.
        :return: a dictionary with the status of the replications and an
            error message
        """
        activeTasks = self.getActiveTasks()
        # filter out any task that is not a database replication
        activeTasks = [task for task in activeTasks if task["type"].lower() == "replication"]

        if len(replicationsList) != len(activeTasks):
            msg = f"Expected to have {len(replicationsList)} replication tasks, "
            msg += f"but only {len(activeTasks)} in CouchDB. "
            msg += f"Current replications are: {activeTasks}"
            return {'status': 'error', 'error_message': msg}

        resp = self.checkReplicationState()
        if resp['status'] != 'ok':
            # then there is a problem, return its status
            return resp

        # finally, check if replications are being updated in a timely fashion
        for replTask in activeTasks:
            if not self.isReplicationOK(replTask):
                source = sanitizeURL(replTask['source'])['url']
                target = sanitizeURL(replTask['target'])['url']
                msg = f"Replication from {source} to {target} is stale and it's last"
                msg += f"update time was at: {replTask.get('updated_on')}"
                resp['status'] = 'error'
                resp['error_message'] += msg
        return resp

    def checkReplicationState(self):
        """
        Check the state of the existent replication tasks.
        NOTE that this can't be done for CouchDB 1.6, since there is
        replication state.

        :return: a dictionary with the status of the replications and an
                 error message
        """
        resp = {'status': 'ok', 'error_message': ""}
        if self.couchVersion == "1.6.1":
            return resp

        for replDoc in self.getSchedulerDocs():
            if replDoc['state'].lower() not in ["pending", "running"]:
                source = sanitizeURL(replDoc['source'])['url']
                target = sanitizeURL(replDoc['target'])['url']
                msg = f"Replication from {source} to {target} is in a bad state: {replDoc.get('state')}; "
                resp['status'] = "error"
                resp['error_message'] += msg
        return resp

    def isReplicationOK(self, replInfo):
        """
        Ensure that the replication document is up-to-date as a
        function of the checkpoint interval.

        :param replInfo: dictionary with the replication information
        :return: True if replication is working fine, otherwise False
        """
        maxUpdateInterval = replInfo['checkpoint_interval'] / 1000
        lastUpdate = replInfo["updated_on"]

        if lastUpdate + maxUpdateInterval > int(time.time()):
            # then it has been recently updated
            return True
        return False
