"""
This unit test isn't really a unit test, more an explaination of how to use the 
CMSCouch library to work with CouchDB. It currently assumes you have a running 
CouchDB instance, and is not going to work in an automated way just yet - we'll 
need to add Couch as an external, include it in start up scripts etc.
"""

from WMCore.Database.CMSCouch import CouchServer
import random
import unittest

class CMSCouchTest(unittest.TestCase):
    test_counter = 0
    def setUp(self):
        # Make an instance of the server
        self.server = CouchServer()
        testname = self.id().split('.')[-1]
        # Create a database, drop an existing one first
        dbname = 'cmscouch_unittest_%s' % testname.lower()
        
        if dbname in self.server.listDatabases():
            self.server.deleteDatabase(dbname)
        
        self.server.createDatabase(dbname)
        self.db = self.server.connectDatabase(dbname)
    
    def tearDown(self):
        if self._exc_info()[0] == None:
            # This test has passed, clean up after it
            testname = self.id().split('.')[-1]
            dbname = 'cmscouch_unittest_%s' % testname.lower()
            self.server.deleteDatabase(dbname)
    
    def testTimeStamping(self):
        doc = {'foo':123, 'bar':456}
        id = self.db.commitOne(doc, timestamp=True, returndocs=True)['id']
        doc = self.db.document(id)
        self.assertTrue('timestamp' in doc.keys())
                
    def testDeleteDoc(self):
        doc = {'foo':123, 'bar':456}
        self.db.commitOne(doc)
        all_docs = self.db.allDocs()
        self.assertEqual(1, len(all_docs['rows']))
        
        # The db.delete_doc is immediate
        id = all_docs['rows'][0]['id']
        self.db.delete_doc(id)
        all_docs = self.db.allDocs()
        self.assertEqual(0, len(all_docs['rows']))
        
    def testDeleteQueuedDocs(self):
        doc1 = {'foo':123, 'bar':456}
        doc2 = {'foo':789, 'bar':101112}
        self.db.queue(doc1)
        self.db.queue(doc2)
        self.db.commit()
        all_docs = self.db.allDocs()
        self.assertEqual(2, len(all_docs['rows']))
        for res in all_docs['rows']:
            id = res['id']
            doc = self.db.document(id)
            self.db.queueDelete(doc)
        all_docs = self.db.allDocs()
        self.assertEqual(2, len(all_docs['rows']))
        
        self.db.commit()
        
        all_docs = self.db.allDocs()
        self.assertEqual(0, len(all_docs['rows']))
        
    def testWriteReadDocNoID(self):
        doc = {}
        
    def testOldTest(self):
        # Views are defined in design documents. You should aim to keep views that 
        # access the same information in the same design doc, to minimise 
        # (de)serialisation. Design documents are dictionaries we post to the server.
        view = {}
        # _ field's are reserved for CouchDB internals - don't use them, and don't over
        # write them unless you know what you are doing. For instance, you may want to 
        # override _id with something you know to be unique in your system...
        view['_id'] = '_design/demo'
        # We could write them in other languages but javascript is fastest, apparently
        view['language'] = 'javascript' 
        # Define a view that lists all documents with a colour property == red
        view['views'] = {'listAllRed':{"map": """function(doc) {
            if (doc.colour == 'red'){
                emit(doc, null);
            }
        }"""}}
        # Define another view that counts the number of documents by colour
        view['views'].update({'countColours':{"map": """function(doc) {
        emit(doc.colour, 1);
        }""", "reduce": """function(doc, values, rereduce) {
            return sum(values);
        }
        """}})
        # Define another view that counts the number of documents by colour, but double 
        # counts red documents
        view['views'].update({'weightedCountColours':{"map": """function(doc) {
            if (doc.colour == 'red'){
                emit(doc.colour, 2);
            } else {
                emit(doc.colour, 1);
            }
        }""", "reduce": """function(doc, values, rereduce) {
            return sum(values);
        }
        """}})
        
        # Send the design document to the server
        self.db.commit(view)
        counts = {}
        counts['red'] = 0
        counts['blue'] = 0
        counts['yellow'] = 0
        counts['green'] = 0
        counts['black'] = 0
        # Make 100 documents
        for i in range(0, 100):
            rand = random.randint(0, 5)
            # documents are dictionaries that we post to the server. CMSCouch will do 
            # the json serialisation for us 
            doc = {}
            if rand < 1:
                doc['colour'] = 'red'
            elif rand < 2:
                doc['colour'] = 'blue'
            elif rand < 3:
                doc['colour'] = 'yellow'
            elif rand < 4:
                doc['colour'] = 'green'
            else:
                doc['colour'] = 'black'
            counts[doc['colour']] += 1
            # These documents are small, so lets do a bulk insert
            self.db.queue(doc)
        # Write everything to the server - we could have queued the view and written the
        # docs and the design doc all at once.
        self.db.commit()
        
        # Now to interact with the data itself
        reddata = self.db.loadView('demo', 'listAllRed')
        self.assertEqual(counts['red'], reddata['total_rows'])
        # This will give us the sum over all docs, we need to group by key to get 
        # something interesting...
        countdata = self.db.loadView('demo', 'countColours')
        self.assertEqual(100, countdata['rows'][0]['value'])
        # Pass in group = True to the loadview call  
        
        countdata = self.db.loadView('demo', 'countColours', {'group': True})
        for row in countdata['rows']:
            self.assertEqual(counts[row['key']], row['value'])
            
        doublecountdata = self.db.loadView('demo', 'weightedCountColours', {'group': True})

        for row in doublecountdata['rows']:
            if row['key'] == 'red':
                self.assertEqual(counts[row['key']] * 2, row['value'])
            else:
                self.assertEqual(counts[row['key']], row['value'])
        
        #Update a document - take the first red document and make it white
        doc = reddata['rows'][0]['key']
        
        # We need to get the document's _id, so we can update it.
        doc['colour'] = 'white'
        # CouchDB is schemaless and duck typed, I can add a new field without breaking
        # existing functionality 
        doc['reason'] = 'white with fright!'
        # Commit the update and add a timestamp
        self.db.commit(doc, timestamp=True)
        
        self.assertNotEqual(doc, self.db.document(doc['_id']))
        self.assertEqual('white', self.db.document(doc['_id'])['colour'])
                
        for doc in reddata['rows']:
            self.db.queueDelete(doc['key'])
        # Bulk delete
        self.db.commit()
        # Remove them permanently
        self.db.compact()
        
        countdata = self.db.loadView('demo', 'countColours', {'group': True})
        for row in countdata['rows']:
            
            if row['key'] == 'red':
                self.assertEqual(0, row['value'])
            elif row['key'] == 'white':
                self.assertEqual(1, row['value'])
            else:
                self.assertEqual(counts[row['key']], row['value'])


if __name__ == "__main__":
    unittest.main()
