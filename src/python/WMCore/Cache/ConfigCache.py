#!/usr/bin/env python
"""
_WMConfigCache_

A simple API for adding/retrieving configurations
"""

import WMCore.Database.CMSCouch as CMSCouch
import urllib

try:
    import hashlib
except:
    import md5 as hashlib

import PSetTweaks.PSetTweak as TweakAPI




class WMConfigCache:
    '''
        __WMConfigCache__

        API for add/retrieving configuration files to/from a CouchDB instance

    '''

    def pullConfig(self, url, dbname, docid, revision = None):
        ''' pulls a document from another WMConfigCache to this one '''

        # get the document
        remoteCache = WMConfigCache(dbname, url)
        document = remoteCache.getDocumentByDocID(docid, revision)

        # add the document to the database
        del document[u'_rev']
        (newid, newrev) = self.database.commit( document )

        # now we have the document, get the attachments
        for attachmentName in document[u'_attachments'].keys():
            attachmentValue = remoteCache.database.getAttachment(newid,
                                                             attachmentName)
            (newid, newrev) = self.database.addAttachment(newid, newrev,
                                                             attachmentValue,
                                                             attachmentName)
        return (newid, newrev)

    def deleteConfig(self, docid):
        '''
            deletes a configuration with a specified docid from the database
        '''
        document = self.database.document(docid)
        self.database.queueDelete(document)
        self.database.commit()

    def addConfig(self, newConfig):
        """
        _addConfig_
        
        Injects a configuration into the cache, returning a tuple with the
        docid and the current revision, in that order. This function should also
        test for already existing documents in the DB and if it's there, just
        return the existing DBID instead of duplicating the rows.
        """
        # The newConfig parameter is a URL suitable for passing to urlopen.
        configString = urllib.urlopen(newConfig).read(-1)
        configMD5 = hashlib.md5(configString).hexdigest()

        viewresult = self.searchByMD5(configMD5)
        if (len(viewresult["rows"]) == 0):
            # The config we want doesn't exist in the database, create it
            document = CMSCouch.Document(None, {"md5_hash": configMD5})
            docsCommitted = self.database.commitOne(document)
            if len(docsCommitted) != 1:
                raise RuntimeError

            retval2 = self.database.addAttachment(docsCommitted[0]["id"],
                                                  docsCommitted[0]["rev"],
                                                  configString,
                                                  "original_script")
            return (retval2["id"], retval2["rev"])
        elif (len(viewresult["rows"]) == 1):
            return (viewresult["rows"][0]["value"]["_id"],
                    viewresult["rows"][0]["value"]["_rev"])
        else:
            raise IndexError, "More than one record has the same MD5"

    def addPickledConfig(self, docid, rev, configPath):
        """
        _addPickledConfig_
        
        """
        configString = urllib.urlopen(configPath).read(-1)
        retval = self.database.addAttachment(docid, rev, configString,
                                             "pickled_script")
        return (retval["id"], retval["rev"])

    def getOriginalConfigByDocID(self, docid):
        """
        _getOriginalConfigByDocID_

        Retrieve the human readable form of a config by the doc ID.
        """
        return self.database.getAttachment(docid, "original_script")

    def getOriginalConfigByHash(self, dochash):
        '''retrieves a configuration by the pset_hash'''
        searchResult = self.searchByHash(dochash)[u'rows']
        if (len(searchResult) == 1):
            # found the configuration
            return self.getConfigByDocID(searchResult[0]['id'],
                                         'original_script')
        else:
            raise IndexError("Too many/few search results (%s) for hash %s" %
                                ( len(searchResult), dochash) )

    def addTweakFile(self, docid, rev, configPath, tweakDict = {"process":{}}):
        """
        _addTweakFile_
        
        Add a tweak file to the given document/config.
        """
        configString = urllib.urlopen(configPath).read(-1)
        self.database.addAttachment(docid, rev, configString, "tweak_file")
        d = self.getDocumentByDocID(docid)
        d.update({"pset_tweak_details" : tweakDict})
        retval = self.database.commitOne(d)[0]
        return (retval["id"], retval["rev"])

    def addTweak(self, docid, rev, tweakDict = {"process":{}}):
         ''' Adds the human-readable script to the given id
             Makes it easy to see what you're doing since
             the pickled version isn't legible
         '''
         d = self.getDocumentByDocID(docid)
         d.update({"pset_tweak_details" : tweakDict})
         commitInfo = self.database.commitOne( d )

         return 



    def getTweak(self, docid):
        """
        _getTweak_

        Retrieve the tweak JSON structure and return it as a PSetTweak
        instance.
        """
        d = self.getDocumentByDocID(docid)
        if d.has_key("pset_tweak_details"):
            return TweakAPI.makeTweakFromJSON(d["pset_tweak_details"])

        return None

    def getTweakFileByDocID(self, docid):
        """
        _getTweakFileByDocID_

        Retrieves a tweak files for the given document ID.
        """
        return self.database.getAttachment(docid, "tweak_file")

    def getTweakFileByHash(self, dochash):
        """
        _getTweakFileByHash_

        Retrieve a tweak file given a pset hash.
        """
        searchResult = self.searchByHash(dochash)["rows"]
        if (len(searchResult) != 1):
            raise IndexError("Too many/few search results (%s) for hash %s" %
                                (len(searchResult), dochash))

        return self.getConfigByDocID(searchResult[0]["id"],
                                     "tweak_file")

    def getDocumentByDocID(self, docid, revid=None):
        '''retrieves a document by its id'''
        return self.database.get("/%s/%s" % (self.dbname, docid), revid)


    def getConfigByDocID(self, docid):
        """
        _getConfigByDocID_

        Retrieve a pickled configuration using the document ID.
        """
        return self.database.getAttachment(docid, "pickled_script")

    def getConfigByHash(self, dochash):
        """
        _getConfigByHash_

        Retrieve a pickled configuration using the pset hash.
        """
        searchResult = self.searchByHash(dochash)["rows"]
        
        if (len(searchResult) == 1):
            return self.getConfigByDocID(searchResult[0]['id'])
        else:
            raise IndexError("Too many/few search results (%s) for hash %s" %
                             (len(searchResult), dochash))
        return

    def getConfigByMD5(self, md5hash):
        '''retrieves a configuration by the md5_hash'''
        searchResult = self.searchByMD5(md5hash)[u'rows']
        if (len(searchResult) == 1):
            # found the configuration
            return self.getConfigByDocID(searchResult[0]['id'])
        else:
            raise IndexError("Too many/few search results (%s) for MD5 %s" %
                                ( len(searchResult), md5hash) )

    def wrapView(self, viewdata):
        '''converts the view return values into Document objects'''
        for row in viewdata[u'rows']:
            row = CMSCouch.Document(None,row)
        return viewdata

    def searchByMD5(self, md5hash):
        '''performs the view for md5 hashes'''
        return self.wrapView( \
                    self.database.loadView( 'documents','by_md5hash' , {}, \
                                            [md5hash] ))
    def searchByHash(self, dochash):
        '''performs the view for pset_hashes'''
        return self.wrapView( \
                    self.database.loadView( 'documents','by_psethash' , {},\
                                            [dochash] ))
    def modifyHash(self, docid, newhash):
        '''changes the hash in an existing document'''
        ourdoc = self.database.document(docid)
        ourdoc[u'pset_hash'] = newhash
        return self.database.commit(ourdoc)

    def tweakRandomSeeds(self, docId):
        """get random seeds view output for doc id"""
        result = \
            self.database.loadView('tweaks', 'randomseeds', {}, [docId])

        if len(result['rows']) == 0:
            return []
        return result['rows'][0]['value']

    def tweakOutputModules(self, docId):
        """get output module info for doc ID"""
        result = \
               self.database.loadView('tweaks', 'outputmodules', {}, [docId])
        output = {}

        for row in result['rows']:
            rowValue = row['value']
            module = rowValue['module']
            content = rowValue['data']
            output[module] = content
        return output


    def __init__(self, dbname2 = 'config_cache', dburl = "http://127.0.0.1:5984"):
        """ attempts to connect to DB, creates if nonexistant:
            TODO: add support for different database URLs
        """
        self.dbname = dbname2
        self.couch = CMSCouch.CouchServer(dburl)
        if self.dbname not in self.couch.listDatabases():
            self.createDatabase()
        self.database = self.couch.connectDatabase(self.dbname)

    def createDatabase(self):
        ''' initializes a non-existant database'''
        database = self.couch.createDatabase(self.dbname)
        hashViewDoc = database.createDesignDoc('documents')
        hashViewDoc['views'] = {
            'by_md5hash':
            { "map": \
              """function(doc) {
              if (doc.md5_hash) {
              emit(doc.md5_hash,{'_id': doc._id, '_rev': doc._rev});
              }
              }
              """ },\
            'by_psethash':
            { "map": \
              """function(doc) {
              if (doc.pset_hash) {
              emit(doc.pset_hash,{'_id': doc._id, '_rev': doc._rev});
              }
              }
              """ },
            }
        database.queue( hashViewDoc )
        database.commit()

        tweakViews = database.createDesignDoc('tweaks')
        tweakViews['views'] ={
            "process" : {
            "map": \
            """
            function(doc) {
              if (doc.pset_tweak_details){
                 if (doc.pset_tweak_details.process){
                    emit(doc._id, doc.pset_tweak_details.process);
                 }
              }
            }
            """
            },
            "randomseeds" :{
            "map":\
            """
            function(doc) {
              if (doc.pset_tweak_details){
                 if (doc.pset_tweak_details.process){
                    var rands = doc.pset_tweak_details.process.RandomNumberGeneratorService;
                    var results = Array();
                    for (var i in rands){
                       if (rands[i].initialSeed){
                          results.push("process.RandomNumberGenerator." + i)
                       }
                    }
                    emit(doc._id, results);
                 }
               }
            }
            """
            },
            "outputmodules" :{
            "map" :\
            """
function(doc) {
  if (doc.pset_tweak_details){
      if (doc.pset_tweak_details.process){
           var process = doc.pset_tweak_details.process;
           for (var i in process){
               var module = process[i];
               if (module.dataset){
                   emit(doc._id, {"module" : i, "data" : module});

               }
           }

      }
   }
}
            """
            },

            }


        database.queue( tweakViews )
        database.commit()
        return database

    def deleteDatabase(self):
        '''deletes an existing database (be careful!)'''
        self.couch.deleteDatabase(self.dbname)





