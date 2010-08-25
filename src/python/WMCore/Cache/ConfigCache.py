import WMCore.Database.CMSCouch as CMSCouch
import time
import urllib
import md5

class WMConfigCache:
    
    def deleteConfig(self, docid):
        '''
            deletes a configuration with a specified docid from the database
        '''
        document = self.database.document(docid)
        self.database.queueDelete(document)
        self.database.commit()
        
    def addConfig(self, new_config):
        '''
            injects a configuration into the cache, returning a tuple with the
            docid and the current revision, in that order. This
            function should also test for already existing documents in the DB
            and if it's there, just return the existing DBID instead of
            duplicating the rows
        '''
        # new_config is a URL suitable for passing to urlopen
        config_string = urllib.urlopen( new_config ).read(-1)
        config_md5    = md5.new( config_string ).hexdigest()
        # check and see if this file is already in the DB
        viewresult = self.searchByMD5( config_md5 )

        if (len(viewresult[u'rows']) == 0):
            # the config we want doesn't exist in the database, create it

            document    = CMSCouch.makeDocument({ "md5_hash": config_md5 })
            commit_info = self.database.commit( document )
            if (commit_info[u'ok'] != True):
                raise RuntimeError
            
            retval2 = self.database.addAttachment( commit_info[u'id'], 
                                         commit_info[u'rev'], 
                                         config_string )
            return ( retval2['id'],
                     retval2['rev'])
            
        elif (len(viewresult[u'rows']) == 1):
            # the config we want is here
            return (viewresult[u'rows'][0][u'value'][u'_id'],
                    viewresult[u'rows'][0][u'value'][u'_rev'])
        else:
            raise IndexError, "More than one record has the same MD5"
            
    
    def getConfigByDocID(self, docid):
        '''retrieves a configuration by the docid'''
        retval = self.database.getAttachment( docid )
        if (len(retval) < 100):
            print retval
        return retval
        
    
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
        for row in viewdata[u'rows']:
            row = CMSCouch.makeDocument(row)
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
    def modifyHash(self, docid, hash):
        '''changes the hash in an existing document'''
        ourdoc = self.database.document(docid)
        ourdoc[u'pset_hash'] = hash
        return self.database.commit(ourdoc)
    
    def __init__(self, dbname2 = 'config_cache', dburl = None):
        """ attempts to connect to DB, creates if nonexistant
            TODO: add support for different database URLs 
        """  
        self.dbname = dbname2
        self.couch = CMSCouch.CouchServer()
        if self.dbname not in self.couch.listDatabases():
            self.createDatabase()
        self.database = self.couch.connectDatabase(self.dbname)
        pass
    
    def createDatabase(self):
        ''' initializes a non-existant database'''
        database = self.couch.createDatabase(self.dbname)
        hash_view_doc = database.createDesignDoc('documents')
        hash_view_doc['views'] = {'by_md5hash': 
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
        database.queue( hash_view_doc )
        database.commit()
        return database
    def deleteDatabase(self):
        '''deletes an existing database (be careful!)'''
        self.couch.deleteDatabase(self.dbname)
        
        

 
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    import WMCore_t.Cache_t.ConfigCache_t
    unittest.main()       
        
    