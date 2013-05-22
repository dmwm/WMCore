#!/usr/bin/env python

"""
Set values in CouchDB request database.

"""


couch_url = "https://cmsweb-testbed.cern.ch/couchdb"
couchdb_name = "reqmgr_workload_cache"
to_set = {"CouchWorkloadDBName": "reqmgr_workload_cache"}

import sys
import os

from WMCore.Database.CMSCouch import CouchServer, Database, Document


def couchdb_setter(db):
    print "Retrieving data from CouchDB ..."
    doc_count = 0
    fields = to_set 
    for row in db.allDocs()["rows"]:
        request_name = row["id"] 
        if request_name.startswith("_design"): continue
        doc_count += 1
        request = db.document(request_name)
        try:
            print "%s: %s" % (request_name, request["CouchWorkloadDBName"])
            continue
        except KeyError:
            print "%s: %s" % (request_name, "n/a")
        print "Setting %s ..." % request_name
        db.updateDocument(request_name, "ReqMgr", "updaterequest", fields=fields)
    print "Queried %s request documents." % doc_count 



def main():
    print "Creating CouchDB database connection ..."
    couchdb = Database(couchdb_name, couch_url)
    couchdb_setter(couchdb)
    

if __name__ == "__main__":
    main()