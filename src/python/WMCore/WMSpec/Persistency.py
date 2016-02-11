#!/usr/bin/env python
"""
_PersistencyHelper_

Util class to provide a common persistency layer for ConfigSection derived
objects, with options to save in different formats

Placeholder for ideas at present....

"""
from __future__ import print_function




import cPickle
import urllib2
from urllib2 import urlopen, Request
from urlparse import urlparse
import json

class PersistencyHelper:
    """
    _PersistencyHelper_

    Save a WMSpec object to a file using cPickle

    Future ideas:
    - cPickle mode: read/write using cPickle
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
        handle = open(filename, 'w')
        #TODO: use different encoding scheme for different extension
        #extension = filename.split(".")[-1].lower()
        cPickle.dump(self.data, handle)
        handle.close()
        return

    def load(self, filename):
        """
        _load_

        UncPickle data from file

        """

        #TODO: currently support both loading from file path or url
        #if there are more things to filter may be separate the load function

        # urllib2 needs a scheme - assume local file if none given
        if not urlparse(filename)[0]:
            filename = 'file:' + filename
            handle = urlopen(Request(filename, headers = {"Accept" : "*/*"}))
            self.data = cPickle.load(handle)
            handle.close()
        elif filename.startswith('file:'):
            handle = urlopen(Request(filename, headers = {"Accept" : "*/*"}))
            self.data = cPickle.load(handle)
            handle.close()
        else:
            # use own request class so we get authentication if needed
            from WMCore.Services.Requests import Requests
            request = Requests(filename)
            data = request.makeRequest('', incoming_headers = {"Accept" : "*/*"})
            self.data = cPickle.loads(data[0])

        #TODO: use different encoding scheme for different extension
        #extension = filename.split(".")[-1].lower()

        return


    def saveCouch(self, couchUrl, couchDBName, metadata={}):
        """ Save this spec in CouchDB.  Returns URL """
        import WMCore.Database.CMSCouch
        server = WMCore.Database.CMSCouch.CouchServer(couchUrl)
        database = server.connectDatabase(couchDBName)
        uri = '/%s/%s' % (couchDBName, self.name())
        specuri = uri + '/spec'
        name = self.name()
        if not database.documentExists(self.name()):
            self.setSpecUrl(couchUrl + specuri)
            doc = database.put(uri, data=metadata, contentType='application/json')
            #doc = database.commitOne(self.name(), metadata)
            rev = doc['rev']
        else:
            #doc = database.get(uri+'?revs=true')
            doc = database.document(self.name())
            rev = doc['_rev']

        #specuriwrev = specuri + '?rev=%s' % rev
        workloadString = cPickle.dumps(self.data)
        #result = database.put(specuriwrev, workloadString, contentType='application/text')
        result = database.addAttachment(name, rev, workloadString, 'spec')
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
