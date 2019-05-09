#!/usr/bin/env python
"""
_PersistencyHelper_

Util class to provide a common persistency layer for ConfigSection derived
objects, with options to save in different formats

Placeholder for ideas at present....

"""
from __future__ import print_function

from urllib2 import urlopen, Request

try:
    from urlparse import urlparse
except ImportError:
    # PY3
    from urllib.parse import urlparse
try:
    import cPickle as pickle
except ImportError:
    import pickle


class PersistencyHelper:
    """
    _PersistencyHelper_

    Save a WMSpec object to a file using pickle

    Future ideas:
    - pickle mode: read/write using pickle
    - python mode: write using pythonise, read using import
       Needs work to preserve tree information
    - gzip mode: also gzip/unzip content if set to True
    - json mode: read/write using json

    """

    def save(self, filename):
        """
        _save_

        Save data to a file
        Saved format is defined depending on the extension
        """
        with open(filename, 'w') as handle:
            # TODO: use different encoding scheme for different extension
            # extension = filename.split(".")[-1].lower()
            pickle.dump(self.data, handle)
        return

    def load(self, filename):
        """
        _load_

        Unpickle data from file

        """

        # TODO: currently support both loading from file path or url
        # if there are more things to filter may be separate the load function

        # urllib2 needs a scheme - assume local file if none given
        if not urlparse(filename)[0]:
            filename = 'file:' + filename
            handle = urlopen(Request(filename, headers={"Accept": "*/*"}))
            self.data = pickle.load(handle)
            handle.close()
        elif filename.startswith('file:'):
            handle = urlopen(Request(filename, headers={"Accept": "*/*"}))
            self.data = pickle.load(handle)
            handle.close()
        else:
            # use own request class so we get authentication if needed
            from WMCore.Services.Requests import Requests
            request = Requests(filename)
            data = request.makeRequest('', incoming_headers={"Accept": "*/*"})
            self.data = pickle.loads(data[0])

        # TODO: use different encoding scheme for different extension
        # extension = filename.split(".")[-1].lower()

        return

    def saveCouch(self, couchUrl, couchDBName, metadata=None):
        """ Save this spec in CouchDB.  Returns URL """
        from WMCore.Database.CMSCouch import CouchServer, CouchInternalServerError
        metadata = metadata or {}
        server = CouchServer(couchUrl)
        database = server.connectDatabase(couchDBName)
        name = self.name()
        uri = '/%s/%s' % (couchDBName, name)
        specuri = uri + '/spec'
        if not database.documentExists(name):
            self.setSpecUrl(couchUrl + specuri)
            doc = database.put(uri, data=metadata, contentType='application/json')
            # doc = database.commitOne(self.name(), metadata)
            rev = doc['rev']
        else:
            # doc = database.get(uri+'?revs=true')
            doc = database.document(name)
            rev = doc['_rev']

        # specuriwrev = specuri + '?rev=%s' % rev
        workloadString = pickle.dumps(self.data)
        # result = database.put(specuriwrev, workloadString, contentType='application/text')
        retval = database.addAttachment(name, rev, workloadString, 'spec')
        if retval.get('ok', False) is not True:
            msg = "Failed to save a spec attachment in CouchDB for %s" % name
            raise CouchInternalServerError(msg, data=None, result=retval)

        url = couchUrl + specuri
        return url

    def splitCouchUrl(self, url):
        """ Splits a URL into baseURL, dbname, and document """
        toks = url.split('/')
        dbname = toks[3]
        # assume that the name "couchdb" is a redirected URL
        if dbname == "couchdb":
            dbname = toks[4]
        toks = url.split('/%s/' % dbname)
        return toks[0], dbname, toks[1]

    def saveCouchUrl(self, url):
        """ Saves the spec to a given Couch URL """
        couchUrl, dbname, doc = self.splitCouchUrl(url)
        return self.saveCouch(couchUrl, dbname)

    def deleteCouch(self, couchUrl, couchDBName, id):
        import WMCore.Database.CMSCouch
        server = WMCore.Database.CMSCouch.CouchServer(couchUrl)
        database = server.connectDatabase(couchDBName)
        # doesn't work
        if not database.documentExists(id):
            print("Could not find document " + id)
            return
        doc = database.delete_doc(id)
        return
