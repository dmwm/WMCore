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
    def testOldTest(self):
        # Make an instance of the server
        server = CouchServer()
        
        # Create a database, drop an existing one first
        dbname = 'cmscouch_unittest'
        
        if dbname in server.listDatabases():
            server.deleteDatabase(dbname)
        
        server.createDatabase(dbname)
        db = server.connectDatabase(dbname)
        
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
        db.commit(view)
        
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
            # These documents are small, so lets do a bulk insert
            db.queue(doc)
        # Write everything to the server - we could have queued the view and written the
        # docs and the design doc all at once.
        db.commit()
        
        # Now to interact with the data itself
        reddata = db.loadView('demo', 'listAllRed')
        print 'there are %s red documents' % reddata['total_rows']
        # This will give us the sum over all docs, we need to group by key to get 
        # something interesting...
        countdata = db.loadView('demo', 'countColours')
        print 'number of documents: %s' % countdata['rows'][0]['value']
        # Pass in group = True to the loadview call  
        print 'number of documents by colour'
        countdata = db.loadView('demo', 'countColours', {'group': True})
        for row in countdata['rows']:
            print 'there are %s %s documents' % (row['value'], row['key'])
            if row['key'] == 'red':
                assert row['value'] == reddata['total_rows']
        print 'now call the double counted red document'
        doublecountdata = db.loadView('demo', 'weightedCountColours', {'group': True})
        for row in doublecountdata['rows']:
            print 'there are %s %s documents' % (row['value'], row['key'])
            if row['key'] == 'red':
                assert row['value'] == 2 * reddata['total_rows']
        
        print "Update a document - take the first red document and make it white"
        doc = reddata['rows'][0]['key']
        # We need to get the document's _id, so we can update it.
        print 'doc before the update: %s' % doc
        doc['colour'] = 'white'
        # CouchDB is schemaless and duck typed, I can add a new field without breaking
        # existing functionality 
        doc['reason'] = 'white with fright!'
        # Commit the update and add a timestamp
        db.commit(doc, timestamp=True)
        
        print 'doc after the update: %s' % db.document(doc['_id'])
        
        print "now delete all the red documents, we emit'ed them as a key in the view"
        for doc in reddata['rows']:
            db.queueDelete(doc['key'])
        # Bulk delete
        db.commit()
        # Remove them permanently
        db.compact()
        
        countdata = db.loadView('demo', 'countColours', {'group': True})
        for row in countdata['rows']:
            if row['value'] == 1:
                print 'there is %s %s document' % (row['value'], row['key'])
            else:
                print 'there are %s %s documents' % (row['value'], row['key'])
            if row['key'] == 'red':
                assert row['value'] == 0
            if row['key'] == 'white':
                assert row['value'] == 1        

if __name__ == "__main__":
    unittest.main()
