#!/usr/bin/env python
'''
    _WMConfigCache_
    
    A simple API for adding/retrieving configurations
    
'''


import WMCore.Database.CMSCouch as CMSCouch
#import WMCore.Database.CMSCouch.Document as Document
import urllib
import md5
__revision__ = "$Id: ConfigCache.py,v 1.8 2009/07/13 18:59:12 meloam Exp $"
__version__ = "$Revision: 1.8 $"

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
        
    def addConfig(self, newConfig ):
        '''
            injects a configuration into the cache, returning a tuple with the
            docid and the current revision, in that order. This
            function should also test for already existing documents in the DB
            and if it's there, just return the existing DBID instead of
            duplicating the rows
        '''
        # new_config is a URL suitable for passing to urlopen
        configString = urllib.urlopen( newConfig ).read(-1)
        configMD5    = md5.new( configString ).hexdigest()
        # check and see if this file is already in the DB
        viewresult = self.searchByMD5( configMD5 )

        if (len(viewresult[u'rows']) == 0):
            # the config we want doesn't exist in the database, create it

            document    = CMSCouch.Document(None,{ "md5_hash": configMD5 })
            commitInfo = self.database.commitOne( document )
            if (commitInfo[u'ok'] != True):
                raise RuntimeError
            
            retval2 = self.database.addAttachment( commitInfo[u'id'], 
                                         commitInfo[u'rev'], 
                                         configString,
                                         'pickled_script')
            return ( retval2['id'],
                     retval2['rev'])
            
        elif (len(viewresult[u'rows']) == 1):
            # the config we want is here
            return (viewresult[u'rows'][0][u'value'][u'_id'],
                    viewresult[u'rows'][0][u'value'][u'_rev'])
        else:
            raise IndexError, "More than one record has the same MD5"
            
    def addOriginalConfig(self, docid, rev, configPath):
        ''' Adds the human-readable script to the given id
            Makes it easy to see what you're doing since
            the pickled version isn't legible
        '''
        configString = urllib.urlopen( configPath ).read(-1)
        retval = self.database.addAttachment( docid,
                                         rev, 
                                         configString,
                                         'original_script')
        return ( retval['id'],
                 retval['rev'])
        
    
    def getOriginalConfigByDocID(self, docid):
        '''retrieves a configuration by the docid'''
        return self.database.getAttachment( docid, 'original_script' )

        
    
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
            
    def addTweakFile(self, docid, rev, configPath):
        ''' Adds the human-readable script to the given id
            Makes it easy to see what you're doing since
            the pickled version isn't legible
        '''
        configString = urllib.urlopen( configPath ).read(-1)
        retval = self.database.addAttachment( docid,
                                         rev, 
                                         configString,
                                         'tweak_file')
        return ( retval['id'],
                 retval['rev'])
        
    
    def getTweakFileByDocID(self, docid):
        '''retrieves a configuration by the docid'''
        return self.database.getAttachment( docid, 'tweak_file' )

        
    
    def getTweakFileByHash(self, dochash):
        '''retrieves a configuration by the pset_hash'''
        searchResult = self.searchByHash(dochash)[u'rows']
        if (len(searchResult) == 1):
            # found the configuration
            return self.getConfigByDocID(searchResult[0]['id'],
                                         'tweak_file')
        else:
            raise IndexError("Too many/few search results (%s) for hash %s" %
                                ( len(searchResult), dochash) )           
    
    def getDocumentByDocID(self, docid, revid=None):
        '''retrieves a document by its id'''
        return self.database.get("/%s/%s" % (self.dbname, docid), revid)
    
               
    def getConfigByDocID(self, docid):
        '''retrieves a configuration by the docid'''
        return self.database.getAttachment( docid,'pickled_script' )
     

    def getConfigByHash(self, dochash):
        '''retrieves a configuration by the pset_hash'''
        searchResult = self.searchByHash(dochash)[u'rows']
        if (len(searchResult) == 1):
            # found the configuration
            return self.getConfigByDocID(searchResult[0]['id'])
        else:
            raise IndexError("Too many/few search results (%s) for hash %s" %
                                ( len(searchResult), dochash) )    
    
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
        print viewdata
        print viewdata.__class__
        for row in viewdata[u'rows']:
            row = CMSCouch.Document(None,row)
        return viewdata
    
    def searchByMD5(self, md5hash):
        '''performs the view for md5 hashes'''
        return self.wrapView( \
                    self.database.loadView( 'documents','by_md5hash' , \
                                            {'key': md5hash } ))
    def searchByHash(self, dochash):
        '''performs the view for pset_hashes'''
        return self.wrapView( \
                    self.database.loadView( 'documents','by_psethash' , \
                                            {'key': dochash } ))    
    def modifyHash(self, docid, newhash):
        '''changes the hash in an existing document'''
        ourdoc = self.database.document(docid)
        ourdoc[u'pset_hash'] = newhash
        return self.database.commit(ourdoc)
    
    def __init__(self, dbname2 = 'config_cache', dburl = None):
        """ attempts to connect to DB, creates if nonexistant
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
        hashViewDoc['views'] = {'by_md5hash': 
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
                     """ }}
        database.queue( hashViewDoc )
        database.commit()
        return database
    def deleteDatabase(self):
        '''deletes an existing database (be careful!)'''
        self.couch.deleteDatabase(self.dbname)
        

 

    